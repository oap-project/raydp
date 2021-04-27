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

from raydp.mpi.mpi_job import MPIJob, MPIType, IntelMPIJob, OpenMPIJob, MPIWorkerPeer


def _get_mpi_type(mpi_type: str) -> MPIType:
    if mpi_type.strip().lower() == "openmpi":
        return MPIType.OPEN_MPI
    elif mpi_type.strip().lower() == "intel_mpi":
        return MPIType.INTEL_MPI
    else:
        return None


def create_mpi_job(job_name: str,
                   world_size: int,
                   num_cpus_per_process: int,
                   num_processes_per_node: int,
                   mpi_script_prepare_fn: Callable = None,
                   timeout: int = 1,
                   mpi_type: str = "intel_mpi",
                   mpi_peer_actor_class=MPIWorkerPeer) -> MPIJob:
    mpi_type = _get_mpi_type(mpi_type)
    if mpi_type == MPIType.OPEN_MPI:
        return OpenMPIJob(mpi_type=MPIType.OPEN_MPI,
                          job_name=job_name,
                          world_size=world_size,
                          num_cpus_per_process=num_cpus_per_process,
                          num_processes_per_node=num_processes_per_node,
                          mpi_script_prepare_fn=mpi_script_prepare_fn,
                          timeout=timeout,
                          peer_actor_class=mpi_peer_actor_class)
    elif mpi_type == MPIType.INTEL_MPI:
        return IntelMPIJob(mpi_type=MPIType.INTEL_MPI,
                           job_name=job_name,
                           world_size=world_size,
                           num_cpus_per_process=num_cpus_per_process,
                           num_processes_per_node=num_processes_per_node,
                           mpi_script_prepare_fn=mpi_script_prepare_fn,
                           timeout=timeout,
                           peer_actor_class=mpi_peer_actor_class)
    else:
        raise Exception(f"MPI type: {mpi_type} not supported now")
