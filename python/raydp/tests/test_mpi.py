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

import sys

import pytest
import ray
import ray._private.services
from ray.util import placement_group, remove_placement_group

# from raydp.mpi import create_mpi_job, MPIJobContext, WorkerContext

@pytest.mark.skip(reason="test CI")
@pytest.mark.timeout(10)
def test_mpi_start(ray_cluster):
    if not ray.worker.global_worker.connected:
        pytest.skip("Skip MPI test if using ray client")
    job = create_mpi_job(job_name="test",
                         world_size=2,
                         num_cpus_per_process=1,
                         num_processes_per_node=2,
                         timeout=5,
                         mpi_type="mpich")
    job.start()

    def func(context: WorkerContext):
        return context.job_id

    results = job.run(func)
    assert len(results) == 2
    assert results[0] == results[1] == "test"

    job.stop()

    # restart
    job.start()

    results = job.run(func)
    assert len(results) == 2
    assert results[0] == results[1] == "test"

    job.stop()

@pytest.mark.skip(reason="test CI")
@pytest.mark.timeout(10)
def test_mpi_get_rank_address(ray_cluster):
    if not ray.worker.global_worker.connected:
        pytest.skip("Skip MPI test if using ray client")
    with create_mpi_job(job_name="test",
                        world_size=2,
                        num_cpus_per_process=1,
                        num_processes_per_node=2,
                        timeout=5,
                        mpi_type="mpich") as job:

        target_address = ray._private.services.get_node_ip_address()
        addresses = job.get_rank_addresses()
        assert len(addresses) == 2
        assert target_address == addresses[0] == addresses[1]

@pytest.mark.skip(reason="test CI")
def test_mpi_with_script_prepare_fn(ray_cluster):
    if not ray.worker.global_worker.connected:
        pytest.skip("Skip MPI test if using ray client")
    def script_prepare_fn(context: MPIJobContext):
        context.add_env("is_test", "True")
        default_script = ["mpirun", "-prepend-rank", "-hosts", ",".join(context.hosts), "-ppn",
                          f"{context.num_procs_per_node}"]
        return default_script

    with create_mpi_job(job_name="test",
                        world_size=2,
                        num_cpus_per_process=1,
                        num_processes_per_node=2,
                        timeout=5,
                        mpi_type="mpich",
                        mpi_script_prepare_fn=script_prepare_fn) as job:

        def f(context: WorkerContext):
            import os
            return os.environ.get("is_test", None)
        results = job.run(f)
        assert len(results) == 2
        assert all([item == "True" for item in results])

@pytest.mark.skip(reason="test CI")
def test_mpi_with_pg(ray_cluster):
    if not ray.worker.global_worker.connected:
        pytest.skip("Skip MPI test if using ray client")
    pg = placement_group(bundles=[{"CPU": 2}], strategy="STRICT_SPREAD")
    with create_mpi_job(job_name="test",
                        world_size=2,
                        num_cpus_per_process=1,
                        num_processes_per_node=2,
                        timeout=5,
                        mpi_type="mpich",
                        placement_group=pg,
                        placement_group_bundle_indexes=[0]) as job:

        def func(context: WorkerContext):
            return context.job_id
        
        results = job.run(func)
        assert len(results) == 2
        assert results[0] == results[1] == "test"

    remove_placement_group(pg)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
