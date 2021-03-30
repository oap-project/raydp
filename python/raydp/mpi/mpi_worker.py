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

import ray
from mpi4py import MPI

from raydp.mpi import constants
from raydp.mpi import network
from raydp.mpi import protocol


def get_rank():
    comm = MPI.COMM_WORLD
    return comm.Get_rank()


class WorkerContext:
    def __init__(self,
                 job_id: str,
                 rank: int,
                 peer_actor: ray.actor.ActorHandle):
        self.job_id = job_id
        self.rank = rank
        self.peer_actor = peer_actor


worker_context = WorkerContext("", 0, None)


class MPIWorker(network.BlockedWorker):
    def __init__(self,
                 name: str,
                 job_id: str,
                 driver_host: str,
                 driver_port: int,
                 timeout: int,
                 max_wait_timeout: int,
                 peer_name: str) -> None:
        super().__init__(name, job_id, driver_host, driver_port, timeout, max_wait_timeout)
        self.peer_name = peer_name
        self.expected_func_id = 0

    def register(self):
        def register_handler(reply: protocol.WorkerRegistered):
            if reply.ray_address is not None:
                # we need to connect to ray
                assert not ray.is_initialized()
                ray.init(address=reply.ray_address,
                         _redis_password=reply.redis_password)
                peer_actor = ray.get_actor(self.peer_name)
                worker_context.peer_actor = peer_actor
            worker_context.job_id = self.job_id
            worker_context.rank = get_rank()
        self.ask(protocol.WorkerRegister(self.job_id,  get_rank(), self.peer_name),
                 register_handler)

    def process_command(self) -> bool:
        value = self.recv()
        return self.handle(value)

    def handle(self, value) -> bool:
        if isinstance(value, protocol.RunFunction):
            try:
                assert value.job_id == self.job_id
                assert value.func_id == self.expected_func_id
                func = value.func
                result = func(worker_context)
                return_msg = protocol.FunctionResult(self.job_id, value.func_id, result, False)
            except Exception as e:
                return_msg = protocol.FunctionResult(self.job_id, value.func_id, e, True)
            finally:
                self.expected_func_id += 1
            self.send(return_msg)
            return True
        elif isinstance(value, protocol.Stop):
            return False


if __name__ == "__main__":
    job_id = network.get_environ_value(constants.MPI_JOB_ID)
    driver_host = network.get_environ_value(constants.MPI_DRIVER_HOST)
    driver_port = int(network.get_environ_value(constants.MPI_DRIVER_PORT))
    peer_name = network.get_environ_value(constants.MPI_WORKER_PEER_NAME)

    timeout = int(os.environ.get(constants.NETWORK_TIME_OUT, "1"))
    max_wait_timeout = int(os.environ.get(constants.MAXIMUM_WAIT_TIME_OUT, "1"))

    client = MPIWorker(job_id, str(get_rank()), driver_host, driver_port,
                       timeout, max_wait_timeout, peer_name)

    # register to driver
    client.register()

    while True:
        if not client.process_command():
            break
    
    client.close()
