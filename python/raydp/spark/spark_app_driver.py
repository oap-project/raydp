import ray
from threading import RLock

def from_spark_safe(df, n):
  rdd_id = df.rdd._id
  app = ray.get_actor("spark_app_driver")
  blocks = [app.get_spark_partition.remote(rdd_id, i) for i in range(n)]
  return blocks

@ray.remote
class SparkAppDriver():
  def __init__(self,
               app_name: str,
               num_executors: int,
               executor_cores: int,
               executor_memory):
    import raydp
    self.session = raydp.init_spark(app_name, num_executors, executor_cores, executor_memory)
    self._lock = RLock()
    self._dfs = {}
    self._saved_blocks = {}
    self._parts_remained = {}

  # inherit this class and overload this function
  # to run your custom workload
  def workload(self):
    from raydp.spark import spark_app_driver
    df = self.session.range(200)
    n = df.rdd.getNumPartitions()
    self._dfs[df.rdd._id] = df
    blocks = spark_app_driver.from_spark_safe(df, n)
    return blocks

  def loseObject(self, object_refs):
    for ref in object_refs:
      ray.internal.free(ref)

  def get_spark_partition(self,
                          rdd_id,
                          partition_id):
    print('hello')
    import raydp
    self._lock.acquire()
    # df has not been saved, save it first
    if rdd_id not in self._saved_blocks:
      df = self._dfs[rdd_id]
      blocks, _, partition_indice = raydp.spark.dataset._save_spark_df_to_object_store(df)
      sorted_blocks = [None] * len(blocks)
      # sort the partitions according to its index
      for i, part_id in enumerate(partition_indice):
        sorted_blocks[part_id] = blocks[i]
      self._saved_blocks[rdd_id] = sorted_blocks
      self._parts_remained[rdd_id] = len(blocks)-1
      self._lock.release()
      return ray.get(self._saved_blocks[rdd_id][partition_id])
    # df has been saved, but has remaining
    # partitions to return
    elif self._parts_remained[rdd_id] > 0:
      self._parts_remained[rdd_id] -= 1
      self._lock.release()
      return ray.get(self._saved_blocks[rdd_id][partition_id])
    # this should not happen unless some blocks got lost and
    # this task is resubmitted. fetch only the lost partition
    else:
      print('recover')
      #recover the particular partition
      return ray.get(raydp.spark.dataset._recover_dataframe_partition(partition_id))

if __name__ == '__main__':
  import time
  ray.init('auto')
  app = SparkAppDriver.options(name="spark_app_driver").remote('test_reconstruction', 5, 2, '1g')
  time.sleep(3)
  blocks = ray.get(app.workload.remote())
  time.sleep(3)
  print(blocks)
  ray.get(app.loseObject.remote([blocks[0]]))
  ds = ray.data.from_arrow_refs(blocks)
  for row in ds.iter_rows():
    print(row)
  # i = 1
  # for block in ds.get_internal_block_refs():
  #   ray.get(block)
  #   print(i)
  #   i = i + 1