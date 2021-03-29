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

from typing import Any, Callable, List


class Protocol:
    def __init__(self, job_id: str):
        self.job_id = job_id


class AgentRegister(Protocol):
    def __init__(self, job_id: str, name: str, command: List[str]) -> None:
        """rsh agent register to driver

        :param job_id: the unique job id
        :param name: the hostname
        :param command: the orted start up command
        """
        super(AgentRegister, self).__init__(job_id)
        self.name = name
        self.command = command


class WorkerRegister(Protocol):
    def __init__(self, job_id: str, rank: int) -> None:
        """mpi process register to driver

        :param job_id: the unique job id
        :param rank: the rank id
        """
        super(WorkerRegister, self).__init__(job_id)
        self.rank = rank


class WorkerRegistered(Protocol):
    def __init__(self,
                 job_id: str,
                 ray_address: str,
                 redis_password: str,
                 peer_name: str) -> None:
        """Driver send back the registered message to MPI processes

        :param ray_address: the ray address to connect
        :param redis_password: the ray redis password
        :param peer_name: the peer actor name
        """
        super(WorkerRegistered, self).__init__(job_id)
        self.ray_address = ray_address
        self.redis_password = redis_password
        self.peer_name = peer_name


class RunFunction(Protocol):
    def __init__(self,
                 job_id: str,
                 func_id: int,
                 func: Callable) -> None:
        """The function to send to MPI processes for executing

        :param func_id: the function id, it should be in order
        :param func: the function to run on each MPI process
        """
        super(RunFunction, self).__init__(job_id)
        self.func_id = func_id
        self.func = func


class FunctionResult(Protocol):
    def __init__(self,
                 job_id: str,
                 func_id: int,
                 result: Any,
                 is_exception: bool) -> None:
        """The function result send from MPI processes to driver

        :param result: the function result
        :param is_exception: whether the function executed failed and return a exception
        """
        super(FunctionResult, self).__init__(job_id)
        self.func_id = func_id
        self.result = result
        self.is_exception = is_exception


class Stop(Protocol):
    pass
