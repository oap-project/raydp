package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl

class RaySchedulerBackend(
    sc: SparkContext,
    scheduler: TaskSchedulerImpl)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {


}
