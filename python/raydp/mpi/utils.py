#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import select
import subprocess
import threading
import time
from typing import List

import grpc
import netifaces


class StoppableThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, *, daemon=None):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


def run_cmd(cmd: str, env, failed_callback):
    # pylint: disable=R1732
    proc = subprocess.Popen(cmd,
                            shell=True,
                            stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            env=env,
                            start_new_session=True)

    def check_failed():
        # check whether the process has finished
        while not threading.current_thread().stopped():
            ret_code = proc.poll()
            if ret_code:
                failed_callback()
                raise Exception(f"mpirun failed: {ret_code}")

            if ret_code == 0:
                break

            time.sleep(1)

    check_thread = StoppableThread(target=check_failed)

    def redirect_stream(streams):
        epoll = select.epoll()
        fileno_mapping = {}
        for stream in streams:
            epoll.register(stream, select.EPOLLIN)
            fileno_mapping[stream.fileno()] = stream
        try:
            while not threading.current_thread().stopped():
                events = epoll.poll(0.5)
                for fileno, events in events:
                    stream = fileno_mapping.get(fileno, None)
                    if not stream:
                        continue
                    line = stream.readline()
                    if not line:
                        epoll.unregister(fileno)
                    else:
                        print(line.decode().strip("\n"))
        finally:
            epoll.close()

    redirect_thread = StoppableThread(target=redirect_stream, args=([proc.stdout, proc.stderr],))
    check_thread.start()
    redirect_thread.start()
    return proc, check_thread, redirect_thread


def create_insecure_channel(address,
                            options=None,
                            compression=None):
    """Disable the http proxy when create channel"""
    # disable http proxy
    if options is not None:
        need_add = True
        for k, v in options:
            if k == "grpc.enable_http_proxy":
                need_add = False
                break
        if need_add:
            options = (*options, ("grpc.enable_http_proxy", 0))
    else:
        options = (("grpc.enable_http_proxy", 0),)

    return grpc.insecure_channel(
        address, options, compression)


def get_environ_value(key: str) -> str:
    """Get value from environ, raise exception if the key not existed"""
    assert key in os.environ, f"{key} should be set in the environ"
    return os.environ[key]


def get_node_ip_address(node_addresses: List[str]) -> str:
    found = None
    for interface in netifaces.interfaces():
        addrs = netifaces.ifaddresses(interface)
        addresses = addrs.get(netifaces.AF_INET, None)
        if not addresses:
            continue
        for inet_addr in addresses:
            address = inet_addr.get("addr", None)
            if address in node_addresses:
                found = address
    return found
