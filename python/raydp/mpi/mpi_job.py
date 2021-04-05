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
import subprocess
import sys
from concurrent import futures
from threading import Thread, RLock, Event
from typing import Any, Callable, Dict

import grpc
import ray
import ray.cloudpickle as cloudpickle

from raydp.mpi import constants
from raydp.mpi.network import network_pb2, network_pb2_grpc
from raydp.mpi.utils import create_insecure_channel, run_cmd, StoppableThread


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
                driver_port: int):
        # prepare the env
        env = os.environ.copy()
        env[constants.MPI_JOB_ID] = self.job_id
        env[constants.MPI_DRIVER_HOST] = str(driver_host)
        env[constants.MPI_DRIVER_PORT] = str(driver_port)
        env[constants.MPI_WORKER_PEER_NAME] = self.name
        node_ip = ray.services.get_node_ip_address()
        env[constants.MPI_WORKER_NODE_IP_ADDRESS] = str(node_ip)
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


class MPIWorkerMeta:
    def __init__(self,
                 dummy_host: str = None,
                 rank: int = None,
                 peer: ray.actor.ActorHandle = None,
                 command: str = None,
                 stub: network_pb2_grpc.WorkerServiceStub = None,
                 worker_ip: str = None,
                 worker_port: int = None):
        self.dummy_host = dummy_host
        self.rank = rank
        self.peer = peer
        self.command = command

        self.stub = stub
        self.worker_ip = worker_ip
        self.worker_port = worker_port


class DriverService(network_pb2_grpc.DriverServiceServicer):
    def __init__(self, driver_service_handler):
        self.driver_service_handler = driver_service_handler

    def RegisterAgent(self, request, context):
        return self.driver_service_handler.handle_register_agent(request)

    def RegisterWorker(self, request, context):
        return self.driver_service_handler.handle_register_worker(request)

    def SendFunctionResult(self, request, context):
        return self.driver_service_handler.handle_send_function_result(request)


class FunctionResults:
    def __init__(self, function_id: int, remaining: int):
        self.function_id = function_id
        self.remaining = remaining
        self.results = [None] * remaining
        self.lock = RLock()
        self.done: Event = Event()


class MPIJob:
    def __init__(self,
                 job_name: str,
                 world_size: int,
                 num_cpus_per_worker: int,
                 mpi_script_prepare_fn: Callable = None,
                 timeout: int = 1,
                 ) -> None:
        self.job_name = job_name
        self.world_size = world_size
        self.num_cpus_per_worker = num_cpus_per_worker
        self.mpi_script_prepare_fn = mpi_script_prepare_fn
        self.timeout = timeout

        self.server_host = None
        self.server_port = None
        self.server = None

        self.workers: Dict[str, MPIWorkerMeta] = {}

        self.mpirun_proc: subprocess.Popen = None
        self.mpirun_check_thread: StoppableThread = None
        self.mpirun_forward_thread: StoppableThread = None

        self.lock = RLock()
        self.registered = 0
        self.register_event = Event()

        self.func_id = 0
        self.func_result: FunctionResults = None
        self.started = False

    def _reset(self):
        if not self.started:
            return
        # send stop to all mpi workers
        if self.workers:
            empty_msg = network_pb2.Empty()
            [meta.stub.Stop(empty_msg)
             for meta in self.workers.values() if meta.stub is not None]
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
        if self.server:
            self.server.stop(None)
            self.server = None
        self.func_id = 0
        self.func_result = None
        self.started = False

    def _start_network_service(self):
        options = (("grpc.enable_http_proxy", 1), )
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                                  options=options)
        network_pb2_grpc.add_DriverServiceServicer_to_server(DriverService(self), self.server)
        # start network server
        self.server_host = ray.services.get_node_ip_address()
        self.server_port = self.server.add_insecure_port(f"{self.server_host}:0")
        self.server.start()

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
        env[constants.MPI_DRIVER_HOST] = str(self.server_host)
        env[constants.MPI_DRIVER_PORT] = str(self.server_port)

        # start up the mpirun in separate thread
        script = subprocess.list2cmdline(default_script)

        (self.mpirun_proc,
         self.mpirun_check_thread,
         self.mpirun_forward_thread) = run_cmd(script, env)

        self._wait_client_register()
        assert len(self.workers) == self.world_size

    def handle_register_agent(self, request: network_pb2.AgentRegisterRequest):
        assert request.job_id == self.job_name
        dummy_host = request.name
        peer_name = f"mpi-{dummy_host}-peer"
        with self.lock:
            assert peer_name not in self.workers
            worker_meta = MPIWorkerMeta()
            worker_meta.dummy_host = dummy_host
            worker_meta.command = request.command
            self.workers[peer_name] = worker_meta
            if len(self.workers) == self.world_size:
                self.register_event.set()
        return network_pb2.AgentRegisterReply(succeed=True)

    def handle_register_worker(self, request: network_pb2.WorkerRegisterRequest):
        assert request.job_id == self.job_name
        with self.lock:
            meta = self.workers[request.peer_name]
            assert meta.stub is None
            meta.rank = request.rank_id
            meta.worker_ip = request.worker_ip
            meta.worker_port = request.worker_port
            self.registered += 1
            if self.registered == self.world_size:
                self.register_event.set()
                self.registered = 0
        return network_pb2.WorkerRegisterReply(ray_address="", redis_password="")

    def handle_send_function_result(self, request: network_pb2.FunctionResult):
        with self.func_result.lock:
            assert self.func_result.function_id == request.func_id
            result = cloudpickle.loads(request.result)
            self.func_result.results[request.rank_id] = result
            self.func_result.remaining -= 1
            if self.func_result.remaining == 0:
                self.func_result.done.set()
        return network_pb2.Empty()

    def _wait_client_register(self):
        self.register_event.wait(self.timeout)
        self.register_event.clear()

    def start(self):
        if self.started:
            return

        try:
            # start network server
            self._start_network_service()
            # start mpirun
            self._start_mpirun()

            # create the WorkerPeer actor
            for name, meta in self.workers.items():
                worker = MPIWorkerPeer.options(name=name,
                                               num_cpus=self.num_cpus_per_worker
                                               ).remote(self.job_name, name, meta.command)
                meta.peer = worker
            # startup mpi worker processes
            ray.get([meta.peer.startup.remote(self.server_host, self.server_port)
                    for meta in self.workers.values()])

            # wait for the worker register
            self._wait_client_register()
            # establish the worker connection

            def connect(meta: MPIWorkerMeta):
                channel = create_insecure_channel(f"{meta.worker_ip}:{meta.worker_port}")
                stub = network_pb2_grpc.WorkerServiceStub(channel)
                assert meta.stub is None
                meta.stub = stub
                return None

            with self.lock:
                [connect(meta) for meta in self.workers.values()]
            self.started = True

        except Exception as e:
            self._reset()
            raise e

    def run(self, mpi_func: Callable, timeout=None) -> Any:
        assert self.started
        func_request = network_pb2.Function(func_id=self.func_id, func=cloudpickle.dumps(mpi_func))
        self.func_result = FunctionResults(self.func_id, self.world_size)
        [meta.stub.RunFunction(func_request) for meta in self.workers.values()]
        self.func_id += 1
        self.func_result.done.wait(timeout)
        return self.func_result.results
        
    def stop(self):
        self._reset()
