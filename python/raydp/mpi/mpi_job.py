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
import subprocess
import sys
import time
from threading import Thread
from typing import Any, Callable, List, Tuple

import ray

from . import constants
from . import network
from . import protocol


class NetworkDriver(network.BlockedDriver):
    def __init__(self, job_id: str, host: str, port: int,
                 timeout: int, max_wait_timeout: int, world_size: int) -> None:
        super().__init__(job_id, host, port, timeout, max_wait_timeout)
        self.world_size = world_size
        self.workers = {}

    def _network_init(self):
        conn = super()._network_init()
        # set the backlog to 2 * world_size, because there are world_size rsh_agent
        # will connect
        conn.listen(self.world_size * 2)
        return conn

    def _wait_one_connection(self, conn_handler: Callable):
        start = time.time()
        dead_line = start + self.max_wait_timeout
        client_conn = None
        client_addr = None
        while time.time() < dead_line:
            try:
                # wait the client connect and raise exception when exceed the max_wait_timeout
                client_conn, client_addr = self.conn.accept()
            except socket.timeout:
                if time.time() < dead_line:
                    continue
                else:
                    raise Exception(f"Wait connection timeout, duration {time.time() - start}")
            break

        assert client_conn is not None
        # wait the register message
        # TODO: we could do this in no-blocking way.
        msg = self._recv_value(client_conn)
        conn_handler(client_conn, client_addr, msg)
    
    def wait_connection(self, n: int, conn_handler: Callable):
        """Wait up to n clients connect to current server

        :param n: the total expected clients to connect
        :param conn_handler: the handler for the client connection
        """
        connected = 0
        while connected < n:
            try:
                self._wait_one_connection(conn_handler)
            except Exception as e:
                raise Exception(
                    f"Waiting {n}({connected} connected) connection failed, exception: {e}")
            connected += 1

    def add_client_conn(self, conn: socket.socket, address):
        """Add one client connection"""
        self.workers[address] = conn

    def broadcast(self, value: Any):
        """Broadcast value to all clients"""
        for conn in self.workers.values():
            self._send_value(conn, value)

    def broadcast_with_reply(self, value: Any, reply_handler: Callable):
        """Broadcast value to all clients and wait for the reply
        
        :param value: the value to broadcast
        :param reply_handler: the reply handler function
        """
        for conn in self.workers.values():
            self._send_value(conn, value)
        for conn in self.workers.values():
            reply = self._recv_value(conn)
            reply_handler(conn, reply)

    def close(self):
        if self.workers:
            [worker.shutdown() for worker in self.workers]
            self.workers = None
        if self.conn is not None:
            self.conn.shutdown()
            self.conn = None


@ray.remote
class MPIWorkerPeer:
    def __init__(self,
                 name: str,
                 mpi_worker_spawn_command: str) -> None:
        self.name = name
        self.mpi_worker_spawn_command: str = mpi_worker_spawn_command
        self.spawn_thread = None

    def startup(self):
        # spawn the MPI worker process
        def spawn_fn():
            subprocess.run(self.mpi_worker_spawn_command,
                           check=True,
                           shell=True)
        self.spawn_thread = Thread(target=spawn_fn)
        self.spawn_thread.start()

    def stop(self, timeout=2):
        if self.spawn_thread is not None and self.spawn_thread.is_alive():
            self.spawn_thread.join(timeout=timeout)
            self.spawn_thread = None


class MPIJob:
    def __init__(self,
                 job_name: str,
                 world_size: int,
                 num_cpus_per_worker: int,
                 mpi_script_prepare_fn: Callable = None) -> None:
        self.job_name = job_name
        self.world_size = world_size
        self.num_cpus_per_worker = num_cpus_per_worker
        self.mpi_script_prepare_fn = mpi_script_prepare_fn
        self.workers = []

        host = ray.services.get_node_ip_address()
        self.network_driver = NetworkDriver(self.job_name, host, 0, 10)
        self.mpi_run_thread = None

        self.dummy_host_to_conn = None

    def _start_mpirun(self):
        # prepare the mpirun script
        rsh_agent = f"'{sys.executable} {constants.RSH_AGENT_PATH}'"
        dummy_hosts = [f"{self.job_name}_host_{rank}" for rank in range(self.world_size)]
        main_class = f"'{sys.executable} {constants.MPI_MAIN_CLASS_PATH}'"
        default_script = ["mpirun", "--allow-run-as-root", "--tag-output",
                          "-bind-to", "none", "-map-by", "slot", "-mca",
                          "pml ob1", "-mca", "btl ^openib", "-mca", "plm rsh",
                          "-mca", "plm_rsh_agent", rsh_agent, "-H", ",".join(dummy_hosts)
                          , "-N", "1", main_class]
        
        if self.mpi_script_prepare_fn is not None:
            default_script = self.mpi_script_prepare_fn(default_script)

        # prepare the mpirun env
        env = os.environ.copy()
        env[constants.MPI_JOB_ID] = self.job_name
        address = self.network_driver.server_address()
        env[constants.MPI_DRIVER_HOST] = address[0]
        env[constants.MPI_DIRVER_PORT] = address[1]
        
        # start up the mpirun in sepearete thread
        def mpi_run_fn():
            subprocess.run(default_script,
                           check=True,
                           shell=True,
                           env=env)
        self.mpi_run_thread = Thread(target=mpi_run_fn)
        self.mpi_run_thread.start()

        # wait for the agent register
        dummy_host_to_conn = {}
        def handle_rsh_agent_register(conn: socket.socket,
                                      address: Tuple,
                                      register_msg: protocol.WorkerRegister):
            assert register_msg.name.startswith("rsh_agent_")
            dummy_host = register_msg.name[len("rsh_agent_"):]
            assert dummy_host not in dummy_host_to_conn
            dummy_host_to_conn[dummy_host] = (conn, address)
        self.network_driver.wait_connection(self.world_size, handle_rsh_agent_register)
        return dummy_host_to_conn

    def start(self):
        if self.dummy_host_conn is not None:
            return

        self.dummy_host_to_conn = self._start_mpirun()
        
        workers = []
        # create the WorkerPeer actor
        for i in range(self.world_size):
            name = f"mpi_{self.job_name}_{i}"
            worker = MPIWorkerPeer.options(name=name,
                                           num_cpus=self.num_cpus_per_worker
                                           ).remote(self.mpi_job_command)
            workers.append(worker)
    
    def run(self, mpi_func: Callable) -> None:
        assert self.network_driver is not None
        self.network_driver.broadcast(protocol.RunFunction(mpi_func))

    def run_with_reply(self, mpi_func) -> List[Any]:
        assert self.network_driver is not None

        results = []
        def reply_handler(conn: socket.socket, reply: Any):
            results.append(reply)
        self.network_driver.broadcast_with_reply(protocol.RunFunction(mpi_func), reply_handler)
        return results
        
    def stop(self):
        if self.network_driver is None:
            return
        # broadcast mpi processes to stop
        self.network_driver.broadcast(protocol.Stop())
        # stop mpi processes peer
        ray.get([worker.stop.remote() for worker in self.workers])
        # stop socket
        self.network_driver.stop()
        self.network_driver = None
        self.workers = []
        self.mpi_run_thread = None
