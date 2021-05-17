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
import threading
from concurrent import futures
from enum import Enum, unique
from threading import RLock, Event
from typing import Any, Callable, List

import grpc
import ray
import ray.cloudpickle as cloudpickle

from raydp.mpi import constants
from raydp.mpi.network import network_pb2, network_pb2_grpc
from raydp.mpi.utils import create_insecure_channel, run_cmd, StoppableThread


@unique
class MPIType(Enum):
    OPEN_MPI = 0
    INTEL_MPI = 1


class MPIWorkerPeer:
    def get_node_ip(self):
        return ray.services.get_node_ip_address()


class MPIWorkerMeta:
    def __init__(self,
                 rank: int = None,
                 stub: network_pb2_grpc.WorkerServiceStub = None,
                 worker_ip: str = None,
                 worker_port: int = None,
                 peer: ray.actor.ActorHandle = None):
        self.rank = rank

        self.stub = stub
        self.worker_ip = worker_ip
        self.worker_port = worker_port
        self.peer = peer


class DriverService(network_pb2_grpc.DriverServiceServicer):
    def __init__(self, driver_service_handler):
        self.driver_service_handler = driver_service_handler

    def RegisterWorker(self, request, context):
        return self.driver_service_handler.handle_register_worker(request)

    def RegisterWorkerService(self, request, context):
        return self.driver_service_handler.handle_register_worker_service(request)

    def RegisterFuncResult(self, request, context):
        return self.driver_service_handler.handle_register_function_result(request)


class FunctionResults:
    def __init__(self, function_id: int, remaining: int):
        self.function_id = function_id
        self.remaining = remaining
        self.results = [None] * remaining
        self.lock = RLock()
        self.done: Event = Event()


class MPIJob:
    def __init__(self,
                 mpi_type: MPIType,
                 job_name: str = None,
                 world_size: int = 1,
                 num_cpus_per_process: int = 1,
                 num_processes_per_node: int = 1,
                 mpi_script_prepare_fn: Callable = None,
                 timeout: int = 1,
                 peer_actor_class=MPIWorkerPeer) -> None:

        assert world_size % num_processes_per_node == 0,\
         (f"world_size: {world_size} should be multiple of num_processes_per_node: "
          f"{num_processes_per_node}")

        self.mpi_type = mpi_type
        self.job_name = job_name
        self.world_size = world_size
        self.num_cpus_per_process = num_cpus_per_process
        self.num_processes_per_node = num_processes_per_node
        self.mpi_script_prepare_fn = mpi_script_prepare_fn
        self.timeout = timeout
        self.peer_actor_class = peer_actor_class

        self.server_host = None
        self.server_port = None
        self.server = None

        self.node_addresses: List[str] = None
        self.peers = None
        self.pg = None
        self.workers: List[MPIWorkerMeta] = [None] * self.world_size

        self.mpirun_proc: subprocess.Popen = None
        self.mpirun_check_thread: StoppableThread = None
        self.mpirun_forward_thread: StoppableThread = None

        self.lock = RLock()
        self.registered = 0
        self.register_event = Event()

        self.func_id = 0
        self.func_result: FunctionResults = None
        self.started = False

    def start(self):
        if self.started:
            return

        try:
            # start network service
            self._start_network_service()
            # start mpirun
            self._start_mpirun()

            # wait for the worker service register
            self._wait_client_register()
            # establish the worker connection

            def connect(meta: MPIWorkerMeta):
                channel = create_insecure_channel(f"{meta.worker_ip}:{meta.worker_port}")
                stub = network_pb2_grpc.WorkerServiceStub(channel)
                assert meta.stub is None
                meta.stub = stub

            with self.lock:
                connected = [connect(meta) for meta in self.workers]
            self.started = True
        except Exception as e:
            self._reset()
            raise e

    def _start_peers(self):
        num_nodes = self.world_size // self.num_processes_per_node
        cpus_per_node = self.num_cpus_per_process * self.num_processes_per_node
        bundles = [{"CPU": cpus_per_node}] * num_nodes
        self.pg = ray.util.placement_group(
            bundles=bundles, strategy="STRICT_SPREAD", name=f"{self.job_name}_pg")
        assert self.pg.wait(self.timeout), f"{ray.util.placement_group_table(self.pg)}"
        # create the WorkerPeer actor
        peers = []
        remote_cls = ray.remote(self.peer_actor_class)
        for node_id in range(num_nodes):
            peer = remote_cls.options(
                placement_group=self.pg, placement_group_bundle_index=node_id
            ).remote()
            peers.append(peer)
        # get the node_ip_address
        self.node_addresses = ray.get([peer.get_node_ip.remote() for peer in peers])
        # holding avoid lost the reference
        self.peers = peers
        return self.node_addresses

    def _start_network_service(self):
        options = (("grpc.enable_http_proxy", 0), )
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                                  options=options)
        network_pb2_grpc.add_DriverServiceServicer_to_server(DriverService(self), self.server)
        # start network server
        self.server_host = ray.services.get_node_ip_address()
        self.server_port = self.server.add_insecure_port(f"{self.server_host}:0")
        self.server.start()

    def get_default_mpirun_script(self, hosts: List[str], num_process_per_node: int) -> List[str]:
        raise NotImplementedError

    def _start_mpirun(self):
        hosts = self._start_peers()
        # prepare the mpirun script
        mpirun_script = self.get_default_mpirun_script(hosts, self.num_processes_per_node)

        if self.mpi_script_prepare_fn is not None:
            mpirun_script = self.mpi_script_prepare_fn(mpirun_script)

        # prepare the mpirun env
        env = os.environ.copy()
        env[constants.MPI_TYPE] = str(self.mpi_type.value)
        env[constants.MPI_JOB_ID] = self.job_name
        env[constants.MPI_DRIVER_HOST] = str(self.server_host)
        env[constants.MPI_DRIVER_PORT] = str(self.server_port)

        # start up the mpirun in separate thread
        script = subprocess.list2cmdline(mpirun_script)

        def failed_callback():
            self.stop()

        (self.mpirun_proc,
         self.mpirun_check_thread,
         self.mpirun_forward_thread) = run_cmd(script, env, failed_callback=failed_callback)

        # wait for the worker register
        self._wait_client_register()

    def handle_register_worker(self, request: network_pb2.RegisterWorkerRequest):
        assert request.job_id == self.job_name
        with self.lock:
            world_rank = request.world_rank
            self.workers[world_rank] = MPIWorkerMeta(rank=world_rank)
            self.registered += 1
            if self.registered == self.world_size:
                self.register_event.set()
                self.registered = 0
        return network_pb2.RegisterWorkerReply(node_addresses=self.node_addresses)

    def handle_register_worker_service(self, request: network_pb2.RegisterWorkerServiceRequest):
        with self.lock:
            world_rank = request.world_rank
            worker = self.workers[world_rank]
            worker.worker_ip = request.worker_ip
            worker.worker_port = request.worker_port
            worker.peer = self.peers[self.node_addresses.index(request.worker_ip)]
            self.registered += 1
            if self.registered == self.world_size:
                self.register_event.set()
                self.registered = 0
        node = ray.worker.global_worker.node
        return network_pb2.RegisterWorkerServiceReply(ray_address=node.redis_address,
                                                      redis_password=node.redis_password)

    def handle_register_function_result(self, request: network_pb2.FunctionResult):
        with self.func_result.lock:
            assert self.func_result.function_id == request.func_id
            result = cloudpickle.loads(request.result)
            self.func_result.results[request.world_rank] = result
            self.func_result.remaining -= 1
            if self.func_result.remaining == 0:
                self.func_result.done.set()
        return network_pb2.Empty()

    def _wait_client_register(self):
        if not self.register_event.wait(self.timeout):
            raise Exception("Timeout exception")
        self.register_event.clear()

    def apply_peers(self, fn: Callable):
        assert self.started
        return fn(self.workers)

    def run(self, mpi_func: Callable, timeout=None) -> Any:
        assert self.started
        func_request = network_pb2.Function(func_id=self.func_id, func=cloudpickle.dumps(mpi_func))
        with self.lock:
            self.func_result = FunctionResults(self.func_id, self.world_size)
        send = [meta.stub.RunFunction(func_request) for meta in self.workers]
        self.func_id += 1
        self.func_result.done.wait(timeout)
        with self.lock:
            if self.func_result:
                results = self.func_result.results
                assert len(results) == self.world_size, "function call failed"
                return self.func_result.results
            else:
                raise Exception("function call failed")

    def get_rank_addresses(self):
        assert self.started
        return [meta.worker_ip for meta in self.workers]

    def _reset(self):
        # send stop to all mpi workers
        if self.workers:
            empty_msg = network_pb2.Empty()

            def send_stop(stub):
                try:
                    stub.Stop(empty_msg)
                except Exception:
                    pass
            for meta in self.workers:
                if not hasattr(meta, "stub") or meta.stub is None:
                    continue
                send_stop(meta.stub)

        # stop mpirun
        if self.mpirun_forward_thread is not None:
            self.mpirun_forward_thread.stop()
            self.mpirun_forward_thread = None
        if (self.mpirun_check_thread is not None
            and threading.current_thread().ident != self.mpirun_check_thread.ident):
            self.mpirun_check_thread.join(1)
            if self.mpirun_check_thread.is_alive():
                # kill the mpirun process
                os.killpg(os.getpgid(self.mpirun_proc.pid), signal.SIGTERM)
                self.mpirun_check_thread.stop()
                self.mpirun_check_thread = None
        self.mpirun_proc = None

        self.workers = [None] * self.world_size
        if self.server:
            self.server.stop(None)
            self.server.wait_for_termination(self.timeout)
            del self.server
            self.server = None
        self.func_id = 0
        with self.lock:
            if self.func_result:
                self.func_result.done.set()
                self.func_result = None
        self.started = False

        if self.peers:
            self.peers = None

        if self.pg:
            ray.util.remove_placement_group(self.pg)
            self.pg = None

    def stop(self):
        self._reset()

    def __del__(self):
        self.stop()


class OpenMPIJob(MPIJob):
    def get_default_mpirun_script(self, hosts: List[str], num_process_per_node: int) -> List[str]:
        default_script = ["mpirun", "--allow-run-as-root", "--tag-output",
                          "-bind-to", "none", "-map-by", "slot", "-mca",
                          "pml", "ob1", "-mca", "btl", "^openib", "-H", ",".join(hosts),
                          "-N", f"{num_process_per_node}", sys.executable,
                          constants.MPI_MAIN_CLASS_PATH]
        return default_script


class IntelMPIJob(MPIJob):
    def get_default_mpirun_script(self, hosts: List[str], num_process_per_node: int) -> List[str]:
        default_script = ["mpirun", "-bind-to", "none", "-map-by", "slot", "-prepend-rank",
                          "-hosts", ",".join(hosts), "-ppn", f"{num_process_per_node}",
                          sys.executable, constants.MPI_MAIN_CLASS_PATH]
        return default_script
