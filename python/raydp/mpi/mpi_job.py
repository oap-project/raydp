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
import signal
import socket
import subprocess
import sys
import time
from threading import Thread
from typing import Any, Callable, Dict, List, Tuple

import ray

from raydp.mpi import constants
from raydp.mpi import network
from raydp.mpi import protocol
from raydp.mpi.utils import run_cmd, StoppableThread


class NetworkDriver(network.BlockedDriver):
    def __init__(self, job_id: str, host: str, port: int,
                 timeout: int, max_wait_timeout: int, world_size: int) -> None:
        super().__init__(job_id, host, port, timeout, max_wait_timeout)
        self.world_size = world_size
        # set the backlog to 2 * world_size, because there are world_size rsh_agent
        # will connect
        self.conn.listen(self.world_size * 2)

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

    def broadcast(self, value: Any, conns: List[socket.socket]):
        """Broadcast value to all clients"""
        for conn in conns:
            self._send_value(conn, value)

    def broadcast_with_reply(self,
                             value: Any,
                             conns: List[socket.socket],
                             reply_handler: Callable):
        """Broadcast value to all clients and wait for the reply
        
        :param value: the value to broadcast
        :param conns: the client connections to broadcast
        :param reply_handler: the reply handler function
        """
        self.broadcast(value, conns)
        for conn in conns:
            reply = self._recv_value(conn)
            reply_handler(conn, reply)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class MPIWorkerMeta:
    def __init__(self,
                 dummy_host: str = None,
                 rank: int = None,
                 conn: socket.socket = None,
                 peer: ray.actor.ActorHandle = None,
                 command: str = None):
        self.dummy_host = dummy_host
        self.rank = rank
        self.conn = conn
        self.peer = peer
        self.command = command


@ray.remote
class MPIWorkerPeer:
    def __init__(self,
                 job_id: str,
                 name: str,
                 mpi_worker_spawn_command: str) -> None:
        self.job_id = job_id
        self.name = name
        self.mpi_worker_spawn_command: str = mpi_worker_spawn_command
        self.spawn_thread = None

    def startup(self,
                driver_host: str,
                driver_port: int,
                network_timeout: int = 1,
                op_timeout: int = 1):
        # prepare the env
        env = os.environ.copy()
        env[constants.MPI_JOB_ID] = self.job_id
        env[constants.MPI_DRIVER_HOST] = str(driver_host)
        env[constants.MPI_DRIVER_PORT] = str(driver_port)
        env[constants.MPI_WORKER_PEER_NAME] = self.name
        env[constants.NETWORK_TIME_OUT] = str(network_timeout)
        env[constants.MAXIMUM_WAIT_TIME_OUT] = str(op_timeout)
        # spawn the MPI worker process

        def spawn_fn():
            proc = subprocess.run(self.mpi_worker_spawn_command,
                                  check=True,
                                  shell=True,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  env=env)
        self.spawn_thread = Thread(target=spawn_fn)
        self.spawn_thread.start()

    def stop(self, timeout=1):
        if self.spawn_thread is not None and self.spawn_thread.is_alive():
            self.spawn_thread.join(timeout=timeout)
            self.spawn_thread = None


class MPIJob:
    def __init__(self,
                 job_name: str,
                 world_size: int,
                 num_cpus_per_worker: int,
                 mpi_script_prepare_fn: Callable = None,
                 network_timeout: int = 1,
                 op_timeout: int = 1,
                 ) -> None:
        self.job_name = job_name
        self.world_size = world_size
        self.num_cpus_per_worker = num_cpus_per_worker
        self.mpi_script_prepare_fn = mpi_script_prepare_fn
        self.network_timeout = network_timeout
        self.op_timeout = op_timeout

        self.workers: Dict[str, MPIWorkerMeta] = {}

        self.network_driver: NetworkDriver = None
        self.mpirun_proc: subprocess.Popen = None
        self.mpirun_check_thread: StoppableThread = None
        self.mpirun_forward_thread: StoppableThread = None
        self.func_id = 0
        self.started = False

    def _reset(self):
        if not self.started:
            return
        # send stop to all mpi workers
        if self.workers:
            valid_conns = [meta.conn for meta in self.workers.values() if meta.conn is not None]
            self.network_driver.broadcast(protocol.Stop(self.job_name), valid_conns)
            # stop peer
            ray.get([meta.peer.stop.remote()
                     for meta in self.workers.values() if meta.peer is not None])
        # stop mpirun
        if self.mpirun_forward_thread is not None:
            self.mpirun_forward_thread.stop()
            self.mpirun_forward_thread = None
        if self.mpirun_check_thread is not None:
            self.mpirun_check_thread.join(1)
            if self.mpirun_check_thread.is_alive():
                # kill the mpirun process
                os.killpg(os.getpgid(self.mpirun_proc.pid), signal.SIGTERM)
                self.mpirun_check_thread.stop()
                self.mpirun_check_thread = None
        self.mpirun_proc = None

        self.workers = {}
        if self.network_driver:
            self.network_driver.close()
            self.network_driver = None
        self.func_id = 0
        self.started = False

    def _start_mpirun(self):
        # prepare the mpirun script
        rsh_agent = f"{sys.executable} {constants.RSH_AGENT_PATH}"
        dummy_hosts = [f"{self.job_name}-host-{rank}" for rank in range(self.world_size)]
        default_script = ["mpirun", "--allow-run-as-root", "--tag-output",
                          "-bind-to", "none", "-map-by", "slot", "-mca",
                          "pml", "ob1", "-mca", "btl", "^openib", "-mca", "plm", "rsh",
                          "-mca", "plm_rsh_agent", rsh_agent, "-H", ",".join(dummy_hosts)
                          , "-N", "1", sys.executable, constants.MPI_MAIN_CLASS_PATH]
        
        if self.mpi_script_prepare_fn is not None:
            default_script = self.mpi_script_prepare_fn(default_script)

        # prepare the mpirun env
        env = os.environ.copy()
        env[constants.MPI_JOB_ID] = self.job_name
        address = self.network_driver.server_address()
        env[constants.MPI_DRIVER_HOST] = str(address[0])
        env[constants.MPI_DRIVER_PORT] = str(address[1])
        env[constants.NETWORK_TIME_OUT] = str(self.network_timeout)
        env[constants.MAXIMUM_WAIT_TIME_OUT] = str(self.op_timeout)

        # start up the mpirun in separate thread
        script = subprocess.list2cmdline(default_script)

        (self.mpirun_proc,
         self.mpirun_check_thread,
         self.mpirun_forward_thread) = run_cmd(script, env)

        # wait for the agent register

        def handle_rsh_agent_register(conn: socket.socket,
                                      conn_address: Tuple,
                                      register_msg: protocol.AgentRegister):
            assert register_msg.job_id == self.job_name
            dummy_host = register_msg.name
            name = f"mpi-{dummy_host}-peer"
            assert name not in self.workers
            worker_meta = MPIWorkerMeta()
            worker_meta.dummy_host = dummy_host
            worker_meta.command = register_msg.command
            self.workers[name] = worker_meta

        self.network_driver.wait_connection(self.world_size, handle_rsh_agent_register)
        assert len(self.workers) == self.world_size

    def start(self):
        if self.started:
            return

        try:
            # start network server
            host = ray.services.get_node_ip_address()
            self.network_driver = NetworkDriver(
                self.job_name, host, 0, self.network_timeout, self.op_timeout, self.world_size)
            # start mpirun
            self._start_mpirun()

            # create the WorkerPeer actor
            for name, meta in self.workers.items():
                worker = MPIWorkerPeer.options(name=name,
                                               num_cpus=self.num_cpus_per_worker
                                               ).remote(self.job_name, name, meta.command)
                meta.peer = worker
            # startup mpi worker processes
            address = self.network_driver.server_address()
            ray.get([meta.peer.startup.remote(address[0], address[1],
                                              self.network_timeout, self.op_timeout)
                    for meta in self.workers.values()])

            # wait mpi worker processes connect
            registered_worker = set()

            def handle_worker_register(conn: socket.socket,
                                       conn_address: Tuple,
                                       register_msg: protocol.WorkerRegister):
                assert register_msg.job_id == self.job_name
                assert register_msg.rank not in registered_worker
                meta = self.workers[register_msg.peer_name]
                meta.rank = register_msg.rank
                meta.conn = conn
                registered_worker.add(register_msg.rank)
                self.network_driver._send_value(conn, protocol.WorkerRegistered(self.job_name, "", ""))

            self.network_driver.wait_connection(self.world_size, handle_worker_register)
            assert len(registered_worker) == self.world_size
            self.started = True

        except Exception as e:
            self._reset()
            raise e

    def _wrap_func(self, func: Callable) -> protocol.RunFunction:
        func = protocol.RunFunction(self.job_name, self.func_id, func)
        self.func_id += 1
        return func

    def run(self, mpi_func: Callable) -> None:
        assert self.network_driver is not None
        conns = [meta.conn for meta in self.workers.values()]
        self.network_driver.broadcast(self._wrap_func(mpi_func), conns)

    def run_with_reply(self, mpi_func) -> List[Any]:
        assert self.network_driver is not None
        conns = [meta.conn for meta in self.workers.values()]

        results = []

        def reply_handler(conn: socket.socket, reply: protocol.FunctionResult):
            if reply.is_exception:
                raise reply.result
            results.append(reply.result)
        self.network_driver.broadcast_with_reply(self._wrap_func(mpi_func), conns, reply_handler)
        return results
        
    def stop(self):
        self._reset()
