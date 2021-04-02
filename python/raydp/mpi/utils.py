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

import grpc


class StoppableThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, *, daemon=None):
        super(StoppableThread, self).__init__(
            group, target, name, args, kwargs, daemon=daemon)
        self._stop_event = threading.Event()

    def run(self) -> None:
        try:
            if self._target:
                self._kwargs["cur_thread"] = self
                self._target(*self._args, **self._kwargs)
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


def run_cmd(cmd: str, env):
    proc = subprocess.Popen(cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            env=env,
                            preexec_fn=os.setsid)

    def check_failed(cur_thread):
        # check whether the process has finished
        while not cur_thread.stopped():
            ret_code = proc.poll()
            if ret_code:
                raise Exception(f"mpirun failed: {ret_code}")

            if ret_code == 0:
                break

            time.sleep(1)

    check_thread = StoppableThread(target=check_failed)

    def redirect_stream(streams, cur_thread):
        epoll = select.epoll()
        for stream in streams:
            epoll.register(stream, select.EPOLLIN)
        try:
            while not cur_thread.stopped():
                events = epoll.poll()
                for fileno, events in events:
                    line = fileno.readline()
                    if not line:
                        epoll.unregister(fileno)
                    else:
                        print(line)
        finally:
            epoll.close()

    redirect_thread = StoppableThread(target=redirect_stream, args=([proc.stdout, proc.stderr]))
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
