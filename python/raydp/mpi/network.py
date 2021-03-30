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
import socket
import struct
import sys
import time
from typing import Any, Callable, List

import ray.cloudpickle as cloudpickle


def get_environ_value(key: str):
    """Get value from environ, raise exception if the key not existed"""
    assert key in os.environ, f"{key} should be set in the environ"
    return os.environ[key]


class NetWorkBase:
    def __init__(self,
                 job_id: str,
                 host: str,
                 port: int,
                 timeout: int,
                 max_wait_timeout: int = 0) -> None:
        """
        :param job_id: the unique job id
        :param host: the host name for listening or connecting
        :param port: the port for listening or connecting
        :param timeout: the network timeout
        :param max_wait_timeout the maximum wait timeout when wait for message
        """
        self.job_id = job_id
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_wait_timeout = max_wait_timeout if max_wait_timeout > 0 else sys.maxsize

        assert self.max_wait_timeout >= self.timeout

        self.conn: socket.socket = None

    def _send_value(self, conn: socket.socket, value: Any) -> None:
        serialized_value = cloudpickle.dumps(value)
        head = struct.pack(">I", len(serialized_value))
        # send the value length + the serialized value
        conn.sendall(head + serialized_value)

    def _recv_value(self, conn: socket.socket) -> Any:
        # receive the serialized value length
        buffer_len = self._recv_all(conn, 4)
        buffer_len = struct.unpack(">I", buffer_len)[0]
        # receive the serialized value
        buffer = self._recv_all(conn, buffer_len, self.max_wait_timeout)
        return cloudpickle.loads(buffer)

    def _recv_all(self,
                  conn: socket.socket,
                  n: int,
                  maximum_time: int,
                  raise_exception: bool = True) -> bytearray:
        """This is a similar method as socket.sendall

        :param conn: the socket connection
        :param n: the expected received bytes
        :param maximum_time: the maximum_time wait for the receive complete, smaller and equal
               zero means wait forever when can not get enough data
        :param raise_exception: whether raise the underlying exception
        :return: the received data in bytearray or None when raise_exception is False
                 and there are exceptions
        """
        buffer = bytearray()
        if maximum_time <= 0:
            maximum_time = sys.maxsize
        start = time.time()
        dead_line = time.time() + maximum_time
        while len(buffer) < n:
            try:
                chunk = conn.recv(n - len(buffer))
            except socket.timeout:
                if time.time() < dead_line:
                    continue
                else:
                    chunk = None
            if not chunk:
                if raise_exception:
                    raise Exception(f"Expect receive {n} bytes, but got {len(buffer)} bytes, "
                                    f"duration: {time.time() - start}")
            buffer.extend(chunk)
        return buffer if len(buffer) > 0 else None

    def close(self):
        raise NotImplementedError


class BlockedDriver(NetWorkBase):
    def __init__(self, job_id: str, host: str, port: int,
                 timeout: int, max_wait_timeout: int) -> None:
        super().__init__(job_id, host, port, timeout, max_wait_timeout)
        self.conn = self._network_init()
    
    def _network_init(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((str(self.host), int(self.port)))
        if self.timeout > 0:
            server.settimeout(self.timeout)
        else:
            server.setblocking(True)
        return server

    def server_address(self):
        return self.conn.getsockname()

    def broadcast(self, value, conns: List[socket.socket]):
        raise NotImplementedError


class BlockedWorker(NetWorkBase):
    def __init__(self,
                 name: str,
                 job_id: str,
                 host: str,
                 port: int,
                 timeout: int,
                 max_wait_timeout: int) -> None:
        super().__init__(job_id, host, port, timeout, max_wait_timeout)
        self.name = name
        self.conn = self._connect()

    def _connect(self) -> socket.socket:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.timeout > 0:
            conn.settimeout(self.timeout)
        else:
            conn.setblocking(True)
        # build connection
        conn.connect((self.host, self.port))
        return conn

    def send(self, value: Any):
        assert self.conn is not None
        self._send_value(self.conn, value)

    def recv(self):
        assert self.conn is not None
        return self._recv_value(self.conn)

    def ask(self, value: Any, reply_handler: Callable):
        assert self.conn is not None
        self.send(value)
        reply = self.recv()
        reply_handler(reply)

    def close(self):
        if self.conn is not None:
            self.conn.shutdown()
            self.conn = None
