# MPI on Ray

RayDP also provides a simple API to running MPI job on top of Ray. Currently, we support three types of MPI: `intel_mpi`, `openmpi` and `MPICH`. To use the following API, make sure you have installed the given type of MPI on each of Ray worker node.

### API

```python
def create_mpi_job(job_name: str,
                   world_size: int,
                   num_cpus_per_process: int,
                   num_processes_per_node: int,
                   mpi_script_prepare_fn: Callable = None,
                   timeout: int = 1,
                   mpi_type: str = "intel_mpi",
                   placement_group=None,
                   placement_group_bundle_indexes: List[int] = None) -> MPIJob:
    """ Create a MPI Job

    :param job_name: the job name
    :param world_size: the world size
    :param num_cpus_per_process: num cpus per process, this used to request resource from Ray
    :param num_processes_per_node: num processes per node
    :param mpi_script_prepare_fn: a function used to create mpi script, it will pass in a
        MPIJobcontext instance. It will use the default script if not provides.
    :param timeout: the timeout used to wait for job creation
    :param mpi_type: the mpi type, now only support openmpi, intel_mpi and mpich
    :param placement_group: the placement_group for request mpi resources
    :param placement_group_bundle_indexes: this should be equal with
        world_size / num_processes_per_node if provides.
    """
```

### Create a simple MPI Job

```python
from raydp.mpi import create_mpi_job, MPIJobContext, WorkerContext

# Define the MPI JOb. We want to create a 4 world_size MPIJob, and each process requires 2 cpus.
# We have set the num_processes_per_node to 2, so the processes will be strictly spread into two nodes.

# You could also to specify the placement group to reserve the resources for MPI job. The num_cpus_per_process 
# will be ignored if the placement group is provided. And the size of
# placement_group_bundle_indexes should be equal with world_size // num_processes_per_node.
job = create_mpi_job(job_name="example",
                     world_size=4,
                     num_cpus_per_process=2,
                     num_processes_per_node=2,
                     timeout=5,
                     mpi_type="intel_mpi",
                     placement_group=None,
                     placement_group_bundle_indexes: List[int] = None)

# Start the MPI Job, this will start up the MPI processes and connect to the ray cluster
job.start()

# define the MPI task function
def func(context: WorkerContext):
    return context.job_id

# run the MPI task, this is a blocking operation. And the results is a world_size array.
results = job.run(func)

# stop the MPI job
job.stop()
```

### Use `with` auto start/stop MPIJob
```python
with create_mpi_job(job_name="example",
                    world_size=4,
                    num_cpus_per_process=2,
                    num_processes_per_node=2,
                    timeout=5,
                    mpi_type="intel_mpi") as job:
    def f(context: WorkerContext):
        return context.job_id
    results = job.run(f)
```

### Specify the MPI script and environments

You could customize the MPI job environments and MPI scritps with `mpi_script_prepare_fn` argument.

```python
def script_prepare_fn(context: MPIJobContext):
    context.add_env("OMP_NUM_THREADS", "2")
    default_script = ["mpirun", "--allow-run-as-root", "--tag-output", "-H",
                      ",".join(context.hosts), "-N", f"{context.num_procs_per_node}"]
    return default_script
  
job = create_mpi_job(job_name="example",
                     world_size=4,
                     num_cpus_per_process=2,
                     num_processes_per_node=2,
                     timeout=5,
                     mpi_type="intel_mpi",
                     mpi_script_prepare_fn=script_prepare_fn)
```
