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
import threading
from concurrent import futures
from queue import Queue

import grpc
import ray
import ray.cloudpickle as cloudpickle

from raydp.mpi import constants, MPIType
from raydp.mpi.network import network_pb2, network_pb2_grpc
from raydp.mpi.utils import create_insecure_channel, get_environ_value, get_node_ip_address, StoppableThread


def get_rank(mpi_type: MPIType):
    if mpi_type == MPIType.OPEN_MPI:
        return int(os.environ["OMPI_COMM_WORLD_RANK"])
    elif mpi_type == MPIType.INTEL_MPI:
        return int(os.environ["PMI_RANK"])
    else:
        try:
            from mpi4py import MPI
            comm = MPI.COMM_WORLD
            return comm.Get_rank()
        except:
            raise Exception(f"Not supported MPI type: {mpi_type}")


class WorkerContext:
    def __init__(self,
                 job_id: str,
                 world_rank: int):
        self.job_id = job_id
        self.world_rank = world_rank


WORLD_RANK = -1
worker_context = WorkerContext("", -1)


class TaskRunner(StoppableThread):
    def __init__(self,
                 task_queue: Queue = None,
                 driver_stub: network_pb2_grpc.DriverServiceStub = None):
        super(TaskRunner, self).__init__(
            group=None, target=None, name="MPI_WORKER_TASK_RUNNER", args=(),
            kwargs=None, daemon=True)
        self.task_queue = task_queue
        self.driver_stub = driver_stub

    def run(self) -> None:
        while not self.stopped():
            expected_func_id, func = self.task_queue.get()
            if func.func_id != expected_func_id:
                raise Exception(f"Rank: {WORLD_RANK}, expected function id: "
                                f"{expected_func_id}, got: {func.func_id}")

            else:
                f = cloudpickle.loads(func.func)
                result = f(worker_context)
                func_result = network_pb2.FunctionResult(world_rank=WORLD_RANK,
                                                         func_id=expected_func_id,
                                                         result=cloudpickle.dumps(result),
                                                         is_exception=False)
                # TODO: catch the stud close exception
                self.driver_stub.RegisterFuncResult(func_result)


class WorkerService(network_pb2_grpc.WorkerServiceServicer):
    def __init__(self, handler):
        self.handler = handler

    def RunFunction(self, request, context):
        return self.handler.handle_run_command(request)

    def Stop(self, request, context):
        return self.handler.handle_stop(request)


class MPIWorker:
    def __init__(self,
                 job_id: str,
                 driver_host: str,
                 driver_port: int) -> None:
        self.job_id = job_id
        self.driver_host = driver_host
        self.driver_port = driver_port

        self.node_ip_address = None

        self.server = None
        self.server_port = None
        self.expected_func_id = 0

        self.driver_stub = None

        self.should_stop = threading.Event()
        self.task_queue = Queue()
        self.task_thread = None

    def _start_network_service(self):
        assert self.node_ip_address is not None
        options = (("grpc.enable_http_proxy", 0),)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                                  options=options)
        network_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(self), self.server)
        self.server_port = self.server.add_insecure_port(f"{self.node_ip_address}:0")
        self.server.start()

    def start(self):
        channel = create_insecure_channel(f"{self.driver_host}:{self.driver_port}")
        stub = network_pb2_grpc.DriverServiceStub(channel)
        register_msg = network_pb2.RegisterWorkerRequest(job_id=self.job_id,
                                                         world_rank=WORLD_RANK)
        reply = stub.RegisterWorker(register_msg)
        self.handle_register_reply(reply)

        self._start_network_service()

        register_msg = network_pb2.RegisterWorkerServiceRequest(world_rank=WORLD_RANK,
                                                                worker_ip=self.node_ip_address,
                                                                worker_port=self.server_port)
        reply = stub.RegisterWorkerService(register_msg)

        # init ray
        ray.init(address=reply.ray_address,
                 _redis_password=reply.redis_password,
                 _node_ip_address=self.node_ip_address)

        self.driver_stub = stub
        self.task_thread = TaskRunner(task_queue=self.task_queue, driver_stub=self.driver_stub)
        self.task_thread.start()

    def wait_for_termination(self):
        self.should_stop.wait()
        self.stop()

    def handle_register_reply(self, reply: network_pb2.RegisterWorkerReply):
        # init ray
        worker_context.job_id = self.job_id
        worker_context.world_rank = WORLD_RANK

        node_ip_address = get_node_ip_address(reply.node_addresses)
        assert node_ip_address is not None, "Could not find the current node_ip_address"
        self.node_ip_address = node_ip_address

    def handle_run_command(self, func: network_pb2.Function):
        self.task_queue.put((self.expected_func_id, func))
        self.expected_func_id += 1
        return network_pb2.Empty()

    def handle_stop(self, request: network_pb2.Empty):
        self.should_stop.set()
        return network_pb2.Empty()

    def stop(self):
        if self.server:
            self.task_thread.stop()
            self.server.stop(None)
            self.server.wait_for_termination()
            self.server = None
            self.server_port = None
            self.expected_func_id = 0


if __name__ == "__main__":
    mpi_type = get_environ_value(constants.MPI_TYPE)
    mpi_type = MPIType(int(mpi_type))
    job_id = get_environ_value(constants.MPI_JOB_ID)
    driver_host = get_environ_value(constants.MPI_DRIVER_HOST)
    driver_port = int(get_environ_value(constants.MPI_DRIVER_PORT))

    WORLD_RANK = get_rank(mpi_type)
    worker = MPIWorker(job_id, driver_host, driver_port)
    worker.start()
    worker.wait_for_termination()
