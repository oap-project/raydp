# Ray Execution & Data Flow

Detailed walkthrough using a concrete example showing internal control and data flow in Ray.

## Simple Ray Example with Internal Flow Explained

Distributed machine learning inference example using Ray and trace exactly what happens internally.[^1][^2][^3]

### The Example Code

```python
import ray
import pandas as pd

# Initialize Ray
ray.init()

# Define a simple model
def load_trained_model():
    def model(batch: pd.DataFrame) -> pd.DataFrame:
        # Predict tip based on passenger count
        predict = batch["passenger_count"] >= 2
        return pd.DataFrame({"score": predict})
    return model

# Define an Actor for batch prediction
@ray.remote
class BatchPredictor:
    def __init__(self, model):
        self.model = model
        self.prediction_count = 0
    
    def predict(self, data_path: str):
        # Read data and make predictions
        df = pd.read_parquet(data_path)
        result = self.model(df)
        self.prediction_count += 1
        return result, self.prediction_count

# Create model and store in object store
model = load_trained_model()
model_ref = ray.put(model)

# Create 3 actor instances
actors = [BatchPredictor.remote(model_ref) for _ in range(3)]

# Process data files in parallel
input_files = ["data1.parquet", "data2.parquet", "data3.parquet"]
futures = [actors[i].predict.remote(input_files[i]) for i in range(3)]

# Get results
results = ray.get(futures)
```


### Internal Control Flow Step-by-Step

#### **Phase 1: Initialization (`ray.init()`)**

When `ray.init()` is called, several processes start on your machine:[^4][^5][^2]

1. **Raylet process** (one per node): Handles local task scheduling and manages the local object store
2. **Worker processes** (typically one per CPU core): Execute tasks and actor methods
3. **Global Control Store (GCS)**: Stores cluster metadata, object locations, and actor registry
4. **Object Store** (one per node): Shared memory region using Apache Arrow/Plasma for zero-copy data sharing[^4][^1]

**What's stored where:**

- GCS: Empty initially, will track all actors and objects
- Object Store: Empty
- Workers: Idle, waiting for tasks


#### **Phase 2: Model Creation (`ray.put(model)`)**

```python
model_ref = ray.put(model)
```

**Control Flow:**

1. Driver process serializes the model function using pickle
2. Driver writes serialized model to **local Object Store** shared memory
3. Object Store returns an `ObjectRef` (e.g., `ObjectRef(a1b2c3d4...)`)
4. Object Store registers this object's location with **GCS**

**Data Storage:**

- **Object Store (Driver Node)**: Contains the serialized model (~few KB)
- **GCS Metadata**: Records `{object_id: a1b2c3d4, location: driver_node, size: 2KB, ref_count: 1}`
- **Driver Memory**: Holds `model_ref` variable pointing to object


#### **Phase 3: Actor Creation (`BatchPredictor.remote(model_ref)`)**

```python
actors = [BatchPredictor.remote(model_ref) for _ in range(3)]
```

**Control Flow for EACH actor:**

1. Driver submits actor creation request to **Local Scheduler (Raylet)**
2. Local Scheduler checks local resources; if sufficient, assigns to a local worker; otherwise forwards to **Global Scheduler**[^2][^4]
3. Scheduler selects Worker Process \#1 for Actor 0
4. Worker \#1 receives:
    - Actor class definition (`BatchPredictor`)
    - Constructor arguments (`model_ref`)
5. Worker \#1 checks if it has `model_ref` locally:
    - **NO** → Queries **GCS** for object location
    - GCS returns: "driver_node's object store"
    - Worker \#1's object store **replicates** model from driver's object store via shared memory (zero-copy on same node)[^6][^1]
6. Worker \#1 deserializes model and calls `__init__(model)`
7. Actor instance is created with:
    - `self.model = model` (in Worker \#1's heap memory)
    - `self.prediction_count = 0` (in Worker \#1's heap memory)
8. Worker \#1 registers actor handle with **GCS**
9. Driver receives `ActorHandle` (e.g., `Actor(BatchPredictor, id=xyz)`)

**After creating 3 actors:**

**Actor Distribution:**

- Worker \#1: Hosts `Actor 0`
- Worker \#2: Hosts `Actor 1`
- Worker \#3: Hosts `Actor 2`

**Actor State Storage (in each worker's process memory, NOT object store):**

```
Worker #1 (Actor 0):
  - self.model = <function> (deserialized from object store)
  - self.prediction_count = 0

Worker #2 (Actor 1):
  - self.model = <function>
  - self.prediction_count = 0

Worker #3 (Actor 2):
  - self.model = <function>
  - self.prediction_count = 0
```

**GCS Registry:**

```
Actor Registry:
  - actor_id: xyz_0, location: worker_1, class: BatchPredictor
  - actor_id: xyz_1, location: worker_2, class: BatchPredictor
  - actor_id: xyz_2, location: worker_3, class: BatchPredictor
```

**Object Store:**

- Still contains the model object (ref_count now = 4: driver + 3 actors)


#### **Phase 4: Method Invocation (`predict.remote()`)**

```python
futures = [actors[i].predict.remote(input_files[i]) for i in range(3)]
```

**Control Flow for Actor 0's `predict.remote("data1.parquet")`:**

1. Driver sends task to **Raylet**: "Execute `predict` method on Actor 0 with argument `'data1.parquet'`"
2. Raylet looks up Actor 0's location in **GCS** → Worker \#1
3. Raylet schedules task on Worker \#1's task queue (actors process methods serially)[^7][^3][^2]
4. Worker \#1 executes:

```python
df = pd.read_parquet("data1.parquet")  # Loads into Worker #1's heap
result = self.model(df)                 # Computation in Worker #1's heap
self.prediction_count += 1              # Updates actor state (heap)
return result, self.prediction_count
```

5. Worker \#1 checks result size:
    - **Small result** (<100KB): Sends directly back to driver's memory
    - **Large result** (>100KB): Stores in Worker \#1's **Object Store** and returns `ObjectRef`
6. In this case, let's say result is large → stored in Object Store
7. Worker \#1's Object Store:
    - Writes result DataFrame to shared memory
    - Registers with **GCS**: `{object_id: result_0, location: worker_1, size: 5MB}`
8. Worker \#1 returns `ObjectRef(result_0)` to driver
9. Driver receives the `ObjectRef` (this is the "future")

**Parallel Execution:**
All 3 actors execute their `predict` methods simultaneously on different workers:[^3]

```
Time 0: All 3 actors start predict() in parallel
├─ Worker #1 (Actor 0): predict("data1.parquet")
├─ Worker #2 (Actor 1): predict("data2.parquet")  
└─ Worker #3 (Actor 2): predict("data3.parquet")

Time T: All complete and store results
```

**Actor State After Execution:**

```
Worker #1 (Actor 0):
  - self.model = <function>
  - self.prediction_count = 1  ← UPDATED

Worker #2 (Actor 1):
  - self.model = <function>
  - self.prediction_count = 1  ← UPDATED

Worker #3 (Actor 2):
  - self.model = <function>
  - self.prediction_count = 1  ← UPDATED
```

**Object Store Contents:**

```
Driver Node Object Store:
  - model (original, 2KB)
  - result_0 (5MB) - if Actor 0 on same node as driver
  - result_1 (5MB) - if Actor 1 on same node as driver
  - result_2 (5MB) - if Actor 2 on same node as driver
```


#### **Phase 5: Result Retrieval (`ray.get(futures)`)**

```python
results = ray.get(futures)
```

**Control Flow:**

1. Driver calls `ray.get([ObjectRef(result_0), ObjectRef(result_1), ObjectRef(result_2)])`
2. For each ObjectRef, driver checks **local Object Store**:
    - If result is local → Deserialize via zero-copy memory mapping[^1]
    - If result is remote → Query **GCS** for location, then fetch from remote worker's Object Store[^8][^6]
3. Assuming all workers are on same node (single machine):
    - Driver's process **memory-maps** each result from Object Store (zero-copy)
4. Driver deserializes the 3 DataFrames into its local Python variables
5. Returns list: `[(df1, 1), (df2, 1), (df3, 1)]`

**Data Transfer Summary:**

- **Same node**: Zero-copy via shared memory mapping (~200 Mbps effective)[^9]
- **Different nodes**: TCP transfer from remote object store to local object store, then zero-copy locally[^6][^9]


### Key Insights on Actor State vs Object Store

**What's Stored in Actor's Process Memory (Heap):**

- Actor instance variables (`self.model`, `self.prediction_count`)
- Local variables during method execution
- Thread/async state if using concurrent actors

**What's Stored in Object Store:**

- Large task/method return values (>100KB)
- Objects explicitly put with `ray.put()`
- Arguments passed between tasks that need to be shared

**Critical Distinction:**

- Actor state is **mutable** and lives in worker's heap
- Object store data is **immutable** and lives in shared memory
- Actors cannot directly access each other's state; they must communicate via object store[^2][^1]


### Visual Summary of Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                       Driver Process                        │
│  model_ref, actors[0-2], futures[0-2]                      │
└────────────┬────────────────────────────────────────────────┘
             │
             ├─── ray.put(model) ───────────────┐
             │                                   ▼
             │                    ┌──────────────────────────┐
             │                    │  Object Store (Shared)   │
             │                    │  • model (2KB)           │
             │                    │  • result_0 (5MB)        │
             │                    │  • result_1 (5MB)        │
             │                    │  • result_2 (5MB)        │
             │                    └──────────────────────────┘
             │                                   ▲
             ├─── BatchPredictor.remote() ──────┤
             │                                   │
    ┌────────┴────────┬─────────────────┬──────┴───────┐
    ▼                 ▼                 ▼               ▼
┌─────────┐      ┌─────────┐      ┌─────────┐    ┌──────────┐
│Worker #1│      │Worker #2│      │Worker #3│    │   GCS    │
│Actor 0  │      │Actor 1  │      │Actor 2  │    │(metadata)│
│         │      │         │      │         │    └──────────┘
│ Heap:   │      │ Heap:   │      │ Heap:   │
│  model  │      │  model  │      │  model  │
│  count=1│      │  count=1│      │  count=1│
└─────────┘      └─────────┘      └─────────┘
```

This architecture allows Ray to efficiently distribute computation while managing both stateful actors and immutable shared data, providing the foundation for scalable ML and distributed applications.[^3][^1][^2]
<span style="display:none">[^10][^11][^12][^13][^14][^15][^16][^17][^18][^19][^20][^21][^22]</span>

<div align="center">⁂</div>

[^1]: https://maxpumperla.com/learning_ray/ch_02_ray_core/

[^2]: https://rise.cs.berkeley.edu/blog/modern-parallel-and-distributed-python-a-quick-tutorial-on-ray/

[^3]: https://www.anyscale.com/blog/model-batch-inference-in-ray-actors-actorpool-and-datasets

[^4]: http://ray-robert.readthedocs.io/en/latest/tutorial.html

[^5]: https://blog.devops.dev/mastering-ray-a-beginners-guide-to-distributed-python-workloads-7d4af4ef341f

[^6]: https://stackoverflow.com/questions/58082023/how-exactly-does-ray-share-data-to-workers

[^7]: https://ray-project.github.io/q4-2021-docs-hackathon/0.4/ray-examples/ray-crash-course/02-Ray-Actors/

[^8]: https://sands.kaust.edu.sa/classes/CS345/S19/papers/ray.pdf

[^9]: https://www.telesens.co/2022/04/23/data-transfer-speed-comparison-ray-plasma-store-vs-s3/

[^10]: https://www.anyscale.com/blog/writing-your-first-distributed-python-application-with-ray

[^11]: https://www.datacamp.com/tutorial/distributed-processing-using-ray-framework-in-python

[^12]: https://ray-project.github.io/q4-2021-docs-hackathon/0.2/ray-distributed-compute/getting-started/

[^13]: https://ray-project.github.io/q4-2021-docs-hackathon/0.4/ray-core/key-concepts/

[^14]: https://domino.ai/blog/ray-tutorial-for-accessing-clusters

[^15]: https://web.engr.oregonstate.edu/~afern/distributed-AI-labs/lab1/ray_tutorial.py

[^16]: https://stackoverflow.com/questions/66893318/how-to-clear-objects-from-the-object-store-in-ray

[^17]: https://colab.research.google.com/github/ray-project/tutorial/blob/master/exercises/colab04-05.ipynb

[^18]: https://github.com/ray-project/ray/issues/51173

[^19]: https://stackoverflow.com/questions/74334814/does-an-instance-of-a-ray-actor-run-in-only-one-process

[^20]: https://github.com/ray-project/ray/issues/15058

[^21]: https://rise.cs.berkeley.edu/blog/ray-tips-for-first-time-users/

[^22]: https://docs.datadoghq.com/integrations/ray/

