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


from typing import Callable, List

from .mpi_job import MPIJob, MPIType, IntelMPIJob, OpenMPIJob, MPICHJob, MPIJobContext
from .mpi_worker import WorkerContext


def _get_mpi_type(mpi_type: str) -> MPIType:
    if mpi_type.strip().lower() == "openmpi":
        return MPIType.OPEN_MPI
    elif mpi_type.strip().lower() == "intel_mpi":
        return MPIType.INTEL_MPI
    elif mpi_type.strip().lower() == "mpich":
        return MPIType.MPICH
    else:
        return None


def create_mpi_job(job_name: str,
                   world_size: int,
                   num_cpus_per_process: int,
                   num_processes_per_node: int,
                   mpi_script_prepare_fn: Callable = None,
                   timeout: int = 1,
                   mpi_type: str = "intel_mpi",
                   placement_group=None,
                   placement_group_bundle_indexes: List[int] = None) -> MPIJob:
    """Create a MPI Job

    :param job_name: the job name
    :param world_size: the world size
    :param num_cpus_per_process: num cpus per process, this used to request resource from Ray
    :param num_processes_per_node: num processes per node
    :param mpi_script_prepare_fn: a function used to create mpi script, it will pass in a
        MPIJobContext instance. It will use the default script if not provides.
    :param timeout: the timeout used to wait for job creation
    :param mpi_type: the mpi type, now only support openmpi, intel_mpi and MPICH
    :param placement_group: the placement_group for request mpi resources
    :param placement_group_bundle_indexes: this should be equal with
        world_size / num_processes_per_node if provides.
    """
    mpi_type = _get_mpi_type(mpi_type)
    if mpi_type == MPIType.OPEN_MPI:
        return OpenMPIJob(mpi_type=MPIType.OPEN_MPI,
                          job_name=job_name,
                          world_size=world_size,
                          num_cpus_per_process=num_cpus_per_process,
                          num_processes_per_node=num_processes_per_node,
                          mpi_script_prepare_fn=mpi_script_prepare_fn,
                          timeout=timeout,
                          placement_group=placement_group,
                          placement_group_bundle_indexes=placement_group_bundle_indexes)
    elif mpi_type == MPIType.INTEL_MPI:
        return IntelMPIJob(mpi_type=MPIType.INTEL_MPI,
                           job_name=job_name,
                           world_size=world_size,
                           num_cpus_per_process=num_cpus_per_process,
                           num_processes_per_node=num_processes_per_node,
                           mpi_script_prepare_fn=mpi_script_prepare_fn,
                           timeout=timeout,
                           placement_group=placement_group,
                           placement_group_bundle_indexes=placement_group_bundle_indexes)
    elif mpi_type == MPIType.MPICH:
        return MPICHJob(mpi_type=MPIType.MPICH,
                        job_name=job_name,
                        world_size=world_size,
                        num_cpus_per_process=num_cpus_per_process,
                        num_processes_per_node=num_processes_per_node,
                        mpi_script_prepare_fn=mpi_script_prepare_fn,
                        timeout=timeout,
                        placement_group=placement_group,
                        placement_group_bundle_indexes=placement_group_bundle_indexes)
    else:
        raise Exception(f"MPI type: {mpi_type} not supported now")


__all__ = ["create_mpi_job", "MPIJobContext", "WorkerContext"]
