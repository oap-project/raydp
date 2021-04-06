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


from typing import Callable

from raydp.mpi.mpi_job import MPIJob, MPIType, IntelMPIJob, OpenMPIJob


def _get_mpi_type(mpi_type: str) -> MPIType:
    if mpi_type.strip().lower() == "openmpi":
        return MPIType.OPEN_MPI
    elif mpi_type.strip().lower() == "intel_mpi":
        return MPIType.INTEL_MPI
    else:
        raise Exception(f"MPI type: {mpi_type} not supported now")


def create_mpi_job(job_name: str,
                   world_size: int,
                   num_cpus_per_worker: int,
                   mpi_script_prepare_fn: Callable = None,
                   timeout: int = 1,
                   mpi_type: str = "intel_mpi") -> MPIJob:
    mpi_type = _get_mpi_type(mpi_type)
    if mpi_type == MPIType.OPEN_MPI:
        return OpenMPIJob(mpi_type=MPIType.OPEN_MPI,
                          job_name=job_name,
                          world_size=world_size,
                          num_cpus_per_worker=num_cpus_per_worker,
                          mpi_script_prepare_fn=mpi_script_prepare_fn,
                          timeout=timeout)
    elif mpi_type == MPIType.INTEL_MPI:
        return IntelMPIJob(mpi_type=MPIType.INTEL_MPI,
                           job_name=job_name,
                           world_size=world_size,
                           num_cpus_per_worker=num_cpus_per_worker,
                           mpi_script_prepare_fn=mpi_script_prepare_fn,
                           timeout=timeout)
