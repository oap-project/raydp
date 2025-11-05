<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

## RayDP Internal Architecture Explained

RayDP bridges Apache Spark and Ray by running Spark executors as Ray actors while enabling efficient data conversion between Spark DataFrames and Ray Datasets through Ray's shared object store.[^1][^2][^3]

### High-Level Architecture

**Core Design Principles:**

RayDP implements a unified compute substrate where Spark runs on top of Ray rather than managing its own cluster:[^2][^3][^1]

1. **Ray as Resource Manager**: Ray replaces Spark's traditional resource manager (YARN/Mesos/Kubernetes). Ray's scheduler allocates resources and manages Spark executor lifecycles.[^3][^1]
2. **Spark Executors as Ray Actors**: Each Spark executor runs inside a Ray actor, which provides isolation, resource guarantees, and statefulness.[^1][^2][^3]
3. **Dual Communication Protocols**:
    - Spark executors communicate with each other using **Spark's internal RPC protocol** for shuffle, broadcast, and task coordination[^1]
    - Data interchange between Spark and Ray uses **Ray's object store** for zero-copy efficiency[^3][^1]

### Detailed Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Ray Cluster                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Ray Global Control Store (GCS)           │  │
│  │  - Tracks all actors (including Spark executors)     │  │
│  │  - Manages object locations                           │  │
│  │  - Coordinates resource allocation                    │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Driver Process (Python)                             │   │
│  │  ├─ PySpark Driver (manages Spark jobs)              │   │
│  │  └─ Ray Driver (submits Ray tasks/actors)            │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────┐  │
│  │  Ray Node 1    │  │  Ray Node 2    │  │  Ray Node N  │  │
│  │                │  │                │  │              │  │
│  │ ┌────────────┐ │  │ ┌────────────┐ │  │┌───────────┐│  │
│  │ │Ray Actor 1 │ │  │ │Ray Actor 2 │ │  ││Ray Actor N││  │
│  │ │            │ │  │ │            │ │  ││           ││  │
│  │ │  Spark     │ │  │ │  Spark     │ │  ││  Spark    ││  │
│  │ │  Executor  │ │  │ │  Executor  │ │  ││  Executor ││  │
│  │ │            │ │  │ │            │ │  ││           ││  │
│  │ │  - JVM     │ │  │ │  - JVM     │ │  ││  - JVM    ││  │
│  │ │  - Cores   │ │  │ │  - Cores   │ │  ││  - Cores  ││  │
│  │ │  - Memory  │ │  │ │  - Memory  │ │  ││  - Memory ││  │
│  │ └────────────┘ │  │ └────────────┘ │  │└───────────┘│  │
│  │       ↕        │  │       ↕        │  │      ↕      │  │
│  │  Object Store  │  │  Object Store  │  │ Object Store│  │
│  │ (Shared Mem)   │  │ (Shared Mem)   │  │(Shared Mem) │  │
│  └────────────────┘  └────────────────┘  └──────────────┘  │
│         ↕   Spark RPC (shuffle/broadcast)    ↕              │
└─────────────────────────────────────────────────────────────┘
```


### How Spark Executors Run as Ray Actors

When you call `raydp.init_spark()`, the following sequence occurs:[^2][^3][^1]

**Step 1: Initialize Ray Cluster**

```python
ray.init(address='auto')
```

- Connects to existing Ray cluster or starts a local one
- Ray GCS becomes active, ready to manage resources

**Step 2: Create Spark on Ray**

```python
spark = raydp.init_spark(
    app_name='RayDP Example',
    num_executors=2,
    executor_cores=2,
    executor_memory='4GB'
)
```

**Internal Flow:**

1. **Driver Setup**: RayDP starts a Spark driver in the current Python process
2. **Resource Reservation**: RayDP calculates total resources needed:
    - 2 executors × 2 cores = 4 CPUs
    - 2 executors × 4GB = 8GB memory
3. **Actor Creation**: For each executor, RayDP creates a Ray actor:

```python
@ray.remote(num_cpus=2, memory=4*1024**3)
class SparkExecutorActor:
    def __init__(self):
        # Start JVM with Spark executor code
        self.jvm = start_jvm_with_spark_executor(...)
    
    def execute_task(self, task_data):
        # Spark tasks execute in this JVM
        return self.jvm.run_task(task_data)
```

4. **Ray Scheduling**: Ray's scheduler places these actors on nodes with available resources
5. **Registration**: Each Spark executor actor registers with the Spark driver using Spark's RPC protocol
6. **Ready State**: Spark cluster is now running on Ray; Spark driver sees N executors available

**Key Characteristics:**

- **Actor Lifecycle**: Spark executors persist as long-lived Ray actors, maintaining state across tasks[^2]
- **Resource Isolation**: Ray guarantees each actor gets its requested CPU/memory allocation
- **Fault Tolerance**: If an actor (executor) fails, Ray can restart it; Spark driver reschedules failed tasks[^1]
- **Communication**: Executors communicate via Spark's shuffle service for data exchange during joins/aggregations[^1]


### Spark DataFrame to Ray Dataset Conversion

The conversion from Spark DataFrame to Ray Dataset is the most critical data interchange operation in RayDP.[^4][^3][^1]

#### Conversion Mechanism: `ray.data.from_spark(df)`

**High-Level Flow:**

```python
# User code
df = spark.range(0, 1000)
ray_ds = ray.data.from_spark(df)
```

**Internal Implementation (Default Non-Fault-Tolerant Mode):**

The conversion uses Spark's `mapPartitions` combined with `ray.put()` to transfer data partition-by-partition into Ray's object store:[^5][^4][^1]

```python
def from_spark(spark_df):
    # Step 1: Convert Spark DataFrame partitions to Arrow format
    # and put each partition into Ray's object store
    
    def partition_to_ray_object(partition_iterator):
        """Runs on each Spark executor (Ray actor)"""
        # Collect partition data
        partition_data = list(partition_iterator)
        
        # Convert to Arrow Table (columnar format)
        arrow_table = pyarrow.Table.from_pydict(partition_data)
        
        # Put into Ray's object store
        # This stores data in local node's shared memory
        object_ref = ray.put(arrow_table)
        
        # Return ObjectRef (not the data itself)
        yield object_ref
    
    # Step 2: Apply function to all partitions
    # This executes in Spark executors (Ray actors)
    object_refs_rdd = spark_df.rdd.mapPartitions(partition_to_ray_object)
    
    # Step 3: Collect all ObjectRefs to driver
    object_refs = object_refs_rdd.collect()
    
    # Step 4: Create Ray Dataset from ObjectRefs
    # Ray Dataset now references data stored in object stores
    return ray.data.from_arrow_refs(object_refs)
```

**Detailed Step-by-Step Flow:**

Let's trace an example with 1000 rows partitioned into 4 Spark partitions:

```
Initial State:
Spark DataFrame (1000 rows, 4 partitions)
├─ Partition 0: rows 0-249   (on Ray Actor/Executor 1)
├─ Partition 1: rows 250-499 (on Ray Actor/Executor 2)
├─ Partition 2: rows 500-749 (on Ray Actor/Executor 3)
└─ Partition 3: rows 750-999 (on Ray Actor/Executor 4)
```

**Conversion Process:**

**Phase 1: Partition-Level Conversion (Parallel on Executors)**

*On Ray Node 1 (Executor 1 - Ray Actor):*

```
1. mapPartitions function receives partition 0 iterator
2. Collect rows 0-249 into memory (in JVM)
3. Convert to PyArrow Table:
   - Serialization: Java objects → Arrow columnar format
   - Uses Arrow IPC format for zero-copy between JVM and Python
4. Call ray.put(arrow_table):
   - Arrow table is serialized using Arrow's format
   - Data is written to Ray's Object Store (shared memory on Node 1)
   - Object Store returns: ObjectRef(abc123)
5. Return ObjectRef(abc123) to Spark
```

*On Ray Node 2 (Executor 2):*

```
Same process for partition 1:
   - Rows 250-499 → Arrow Table → ray.put()
   - Returns: ObjectRef(def456)
```

*Similar for Nodes 3 and 4...*

**Phase 2: Collect ObjectRefs to Driver**

```
Driver Process:
object_refs = [
    ObjectRef(abc123),  # Points to data on Node 1's object store
    ObjectRef(def456),  # Points to data on Node 2's object store
    ObjectRef(ghi789),  # Points to data on Node 3's object store
    ObjectRef(jkl012)   # Points to data on Node 4's object store
]
```

**Important**: At this stage, **no actual data is transferred**—only lightweight ObjectRefs (essentially pointers) are collected to the driver.[^6][^7]

**Phase 3: Create Ray Dataset**

```python
ray_ds = ray.data.from_arrow_refs(object_refs)
```

Ray Dataset is constructed as a lazy, distributed collection of Arrow table references:

```
Ray Dataset Structure:
├─ Block 0: ObjectRef(abc123) @ Node 1 Object Store
├─ Block 1: ObjectRef(def456) @ Node 2 Object Store
├─ Block 2: ObjectRef(ghi789) @ Node 3 Object Store
└─ Block 3: ObjectRef(jkl012) @ Node 4 Object Store
```


### Data Storage and Zero-Copy Mechanics

**Where Data Lives After Conversion:**

```
┌─────────────────────────────────────────────────────────┐
│ Ray Node 1                                              │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Ray Actor (Spark Executor 1)                     │  │
│  │  - JVM Memory: Spark partition 0 data (original) │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Ray Object Store (Shared Memory)                 │  │
│  │  ObjectRef(abc123):                              │  │
│  │    Arrow Table (rows 0-249)                      │  │
│  │    Format: Apache Arrow IPC                      │  │
│  │    Size: ~X MB                                   │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

**Zero-Copy Properties:**

1. **Within Same Node**: When Ray tasks on Node 1 access `ObjectRef(abc123)`, they memory-map the Arrow table directly from shared memory—no serialization or copying.[^8][^9][^6]
2. **Cross-Node Access**: If a Ray task on Node 2 needs `ObjectRef(abc123)`:
    - Ray automatically replicates the object from Node 1's object store to Node 2's object store
    - After replication, access is zero-copy within Node 2[^7][^10]
3. **Arrow Format Advantage**: Arrow's columnar format is self-describing and language-agnostic, enabling zero-copy reads across Python, C++, and Java without deserialization overhead.[^9][^6]

### Ray Dataset to Spark DataFrame Conversion

The reverse conversion (`ds.to_spark(spark)`) follows a different pattern:[^11][^12]

**Method 1: Direct Conversion (when supported)**

```python
ray_ds = ray.data.from_items([{"id": i} for i in range(1000)])
spark_df = ray_ds.to_spark(spark)
```

**Internal Flow:**

```python
def to_spark(ray_dataset, spark_session):
    # Step 1: Write Ray Dataset to intermediate storage
    # (either in-memory or temp files)
    
    # Option A: For small datasets - use Spark's createDataFrame
    collected_data = ray.get(ray_dataset._block_refs)
    arrow_tables = [block for block in collected_data]
    
    # Convert Arrow to Pandas (zero-copy where possible)
    pandas_dfs = [table.to_pandas() for table in arrow_tables]
    
    # Spark createDataFrame from Pandas
    return spark_session.createDataFrame(pd.concat(pandas_dfs))
```

**Method 2: Intermediate Storage (for large datasets)**[^12][^11]

```python
# Write Ray Dataset to Parquet/temp storage
ray_ds.write_parquet("/tmp/ray_output")

# Read into Spark from storage
spark_df = spark.read.parquet("/tmp/ray_output")
```

This avoids driver memory bottlenecks for large datasets.[^11][^12]

### Data Format and Serialization

**Apache Arrow as Interchange Format:**

RayDP relies heavily on Apache Arrow for efficient data interchange:[^13][^6][^9]

- **Columnar Memory Layout**: Arrow stores data in contiguous memory columns, optimized for analytics
- **Zero-Copy Deserialization**: Arrow data can be read without parsing—processes map memory directly[^6][^9]
- **Language Interoperability**: Arrow format is understood by Java (Spark), Python (Ray/Pandas), and C++[^13][^6]
- **Shared Memory Friendly**: Arrow's immutable buffers are perfect for Ray's object store[^6]

**Serialization Path:**

```
Spark DataFrame (JVM)
    ↓ (Arrow IPC)
Arrow Table (Python/Spark Executor)
    ↓ (ray.put - Arrow serialization)
Ray Object Store (Shared Memory)
    ↓ (zero-copy memory map)
Ray Dataset Blocks (Arrow format)
    ↓ (consumed by Ray Train/XGBoost)
ML Framework (PyTorch/TensorFlow tensors)
```


### Performance Characteristics

**Advantages of RayDP's Design:**

1. **Minimal Data Movement**: Data stays in Arrow format from Spark through Ray to ML frameworks[^14][^6]
2. **Locality-Aware**: Data partitions remain on the same nodes where Spark executors (Ray actors) ran[^14][^2]
3. **Parallel Transfer**: All partitions convert to Ray format simultaneously across executors[^1]
4. **Memory Efficiency**: Object store uses shared memory; multiple Ray tasks can read the same data without copies[^6]

**Limitations:**

1. **Not Fault-Tolerant by Default**: Objects created with `ray.put()` are not recoverable—if a node fails, data is lost[^4][^1]
2. **Memory Pressure**: Large DataFrames can fill object store; Ray spills to disk but adds latency[^1]
3. **Dependency on Spark**: Ray Dataset becomes invalid after `raydp.stop_spark()` unless using `cleanup_data=False`[^3][^1]

### Fault-Tolerant Conversion (Experimental)

RayDP offers an experimental fault-tolerant mode using `from_spark_recoverable()`:[^1]

```python
spark = raydp.init_spark(..., fault_tolerance_mode=True)
df = spark.range(100000)
ds = raydp.spark.from_spark_recoverable(df)
```

**How It Works:**

- Persists Spark DataFrame to Spark's storage (memory/disk) with configurable `storage_level`
- Conversion creates Ray tasks (not just `ray.put`) that can re-read from Spark if data is lost
- Ray maintains lineage: if object store loses data, Ray re-executes the conversion task from persisted Spark data[^1]

This adds overhead but enables recovery from node failures during ML training.[^1]

### Summary

RayDP's architecture elegantly unifies Spark and Ray by treating Spark executors as Ray actors and using Ray's object store as a high-performance data interchange layer. The `mapPartitions` + `ray.put()` mechanism combined with Apache Arrow's zero-copy format enables efficient, parallel data conversion with minimal overhead, making it practical to build end-to-end ETL-to-ML pipelines in a single Python program.[^2][^3][^6][^1]
<span style="display:none">[^15][^16][^17][^18][^19][^20][^21][^22][^23][^24][^25][^26][^27][^28][^29][^30][^31][^32][^33][^34]</span>

<div align="center">⁂</div>

[^1]: https://github.com/oap-project/raydp

[^2]: https://www.samsara.com/blog/building-a-modern-machine-learning-platform-with-ray

[^3]: https://pypi.org/project/raydp/0.5.0/

[^4]: https://pypi.org/project/raydp-nightly/2021.7.21.dev0/

[^5]: https://blog.csdn.net/weixin_45681127/article/details/119916961

[^6]: https://arrow.apache.org/blog/2017/10/15/fast-python-serialization-with-ray-and-arrow/

[^7]: https://stackoverflow.com/questions/58082023/how-exactly-does-ray-share-data-to-workers

[^8]: https://www.reddit.com/r/datascience/comments/ptmxr1/how_does_the_arrow_package_achieve_zerocopy/

[^9]: https://www.datacamp.com/tutorial/apache-arrow

[^10]: https://sands.kaust.edu.sa/classes/CS345/S19/papers/ray.pdf

[^11]: https://learn.microsoft.com/en-us/azure/databricks/machine-learning/ray/connect-spark-ray

[^12]: https://docs.databricks.com/aws/en/machine-learning/ray/connect-spark-ray

[^13]: https://arrow.apache.org/docs/python/interchange_protocol.html

[^14]: https://community.intel.com/t5/Blogs/Tech-Innovation/Cloud/Intel-Optimization-for-XGBoost-on-Ray-with-RayDP-Delivers-Better/post/1535578

[^15]: https://docs.databricks.com/aws/en/machine-learning/ray/

[^16]: https://www.reddit.com/r/dataengineering/comments/1jjzl1u/need_help_optimizing_35tb_pyspark_job_on_ray/

[^17]: https://stackoverflow.com/questions/73973111/using-ray-to-transfer-data-from-spark-to-ray-datasets

[^18]: https://www.thomasjpfan.com/2023/05/accessing-data-from-pythons-dataframe-interchange-protocol/

[^19]: https://cloud.google.com/vertex-ai/docs/open-source/ray-on-vertex-ai/run-spark-on-ray

[^20]: https://www.anyscale.com/blog/data-processing-support-in-ray

[^21]: https://data-apis.org/dataframe-protocol/latest/purpose_and_scope.html

[^22]: https://www.shakudo.io/blog/apache-spark-intro-on-shakudo

[^23]: https://speakerdeck.com/anyscale/raydp-build-large-scale-end-to-end-data-analytics-and-ai-pipelines-using-spark-and-ray-carson-wang-intel-data-analytics-software-group

[^24]: https://www.anyscale.com/events/raydp-build-large-scale-end-to-end-data-analytics-and-ai-pipelines-using-spark-and-ray

[^25]: https://github.com/ray-project/ray/issues/32313

[^26]: https://stackoverflow.com/questions/65509407/apache-arrow-getting-vectors-from-java-in-python-with-zero-copy

[^27]: https://arxiv.org/abs/2404.03030

[^28]: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.mapPartitions.html

[^29]: https://discourse.julialang.org/t/how-well-apache-arrow-s-zero-copy-methodology-is-supported/59797

[^30]: https://spark.apache.org/docs/latest/rdd-programming-guide.html

[^31]: https://github.com/oap-project/raydp/issues/409

[^32]: https://docs.databricks.com/aws/en/machine-learning/ray/spark-ray-overview

[^33]: https://github.com/ray-project/ray/issues/31455

[^34]: https://learn.microsoft.com/en-us/azure/databricks/machine-learning/ray/spark-ray-overview

