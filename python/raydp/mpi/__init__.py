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

from enum import Enum, unique
from typing import Callable

from raydp.mpi.mpi_job import MPIJob, OpenMPIJob


@unique
class MPIType(Enum):
    OPEN_MPI = 0
    INTEL_MPI = 1


def create_mpi_job(job_name: str,
                   world_size: int,
                   num_cpus_per_worker: int,
                   mpi_script_prepare_fn: Callable = None,
                   timeout: int = 1,
                   mpi_type: MPIType = MPIType.OPEN_MPI) -> MPIJob:
    if mpi_type == MPIType.OPEN_MPI:
        return OpenMPIJob(job_name=job_name,
                          world_size=world_size,
                          num_cpus_per_worker=num_cpus_per_worker,
                          mpi_script_prepare_fn=mpi_script_prepare_fn,
                          timeout=timeout)
    else:
        raise Exception("Not supported now")
