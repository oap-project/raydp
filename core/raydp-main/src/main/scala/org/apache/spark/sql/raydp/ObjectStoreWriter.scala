/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.raydp


import com.intel.raydp.shims.SparkShimLoader

import java.io.ByteArrayOutputStream
import java.util.{List, UUID}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.function.{Function => JFunction}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import io.ray.api.{ActorHandle, ObjectRef, PyActorHandle, Ray}
import io.ray.runtime.AbstractRayRuntime
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.{RayDPException, SparkContext}
import org.apache.spark.deploy.raydp._
import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.raydp.{RayDPUtils, RayExecutorUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.python.BatchIterator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * A batch of record that has been wrote into Ray object store.
 * @param ownerAddress the owner address of the ray worker
 * @param objectId the ObjectId for the stored data
 * @param numRecords the number of records for the stored data
 */
case class RecordBatch(
    ownerAddress: Array[Byte],
    objectId: Array[Byte],
    numRecords: Int)

class ObjectStoreWriter(@transient val df: DataFrame) extends Serializable {

  val uuid: UUID = ObjectStoreWriter.dfToId.getOrElseUpdate(df, UUID.randomUUID())

  def writeToRay(
      data: Array[Byte],
      numRecords: Int,
      queue: ObjectRefHolder.Queue,
      ownerName: String): RecordBatch = {

    var objectRef: ObjectRef[Array[Byte]] = null
    if (ownerName == "") {
      objectRef = Ray.put(data)
    } else {
      var dataOwner: PyActorHandle = Ray.getActor(ownerName).get()
      objectRef = Ray.put(data, dataOwner)
    }

    // add the objectRef to the objectRefHolder to avoid reference GC
    queue.add(objectRef)
    val objectRefImpl = RayDPUtils.convert(objectRef)
    val objectId = objectRefImpl.getId
    val runtime = Ray.internal.asInstanceOf[AbstractRayRuntime]
    val addressInfo = runtime.getObjectStore.getOwnershipInfo(objectId)
    RecordBatch(addressInfo, objectId.getBytes, numRecords)
  }

  /**
   * Save the DataFrame to Ray object store with Apache Arrow format.
   */
  def save(useBatch: Boolean, ownerName: String): List[RecordBatch] = {
    val conf = df.queryExecution.sparkSession.sessionState.conf
    val timeZoneId = conf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE)
    var batchSize = conf.getConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH)
    if (!useBatch) {
      batchSize = 0
    }
    val schema = df.schema

    val objectIds = df.queryExecution.toRdd.mapPartitions{ iter =>
      val queue = ObjectRefHolder.getQueue(uuid)

      // DO NOT use iter.grouped(). See BatchIterator.
      val batchIter = if (batchSize > 0) {
        new BatchIterator(iter, batchSize)
      } else {
        Iterator(iter)
      }

      val arrowSchema = SparkShimLoader.getSparkShims.toArrowSchema(schema, timeZoneId)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"ray object store writer", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val results = new ArrayBuffer[RecordBatch]()

      val byteOut = new ByteArrayOutputStream()
      val arrowWriter = ArrowWriter.create(root)
      var numRecords: Int = 0

      Utils.tryWithSafeFinally {
        while (batchIter.hasNext) {
          // reset the state
          numRecords = 0
          byteOut.reset()
          arrowWriter.reset()

          // write out the schema meta data
          val writer = new ArrowStreamWriter(root, null, byteOut)
          writer.start()

          // get the next record batch
          val nextBatch = batchIter.next()

          while (nextBatch.hasNext) {
            numRecords += 1
            arrowWriter.write(nextBatch.next())
          }

          // set the write record count
          arrowWriter.finish()
          // write out the record batch to the underlying out
          writer.writeBatch()

          // get the wrote ByteArray and save to Ray ObjectStore
          val byteArray = byteOut.toByteArray
          results += writeToRay(byteArray, numRecords, queue, ownerName)
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        }
        arrowWriter.reset()
        byteOut.close()
      } {
        // If we close root and allocator in TaskCompletionListener, there could be a race
        // condition where the writer thread keeps writing to the VectorSchemaRoot while
        // it's being closed by the TaskCompletion listener.
        // Closing root and allocator here is cleaner because root and allocator is owned
        // by the writer thread and is only visible to the writer thread.
        //
        // If the writer thread is interrupted by TaskCompletionListener, it should either
        // (1) in the try block, in which case it will get an InterruptedException when
        // performing io, and goes into the finally block or (2) in the finally block,
        // in which case it will ignore the interruption and close the resources.

        root.close()
        allocator.close()
      }

      results.toIterator
    }.collect()
    objectIds.toSeq.asJava
  }

  /**
   * For test.
   */
  def getRandomRef(): List[Array[Byte]] = {

    df.queryExecution.toRdd.mapPartitions { _ =>
      Iterator(ObjectRefHolder.getRandom(uuid))
    }.collect().toSeq.asJava
  }

  def clean(): Unit = {
    ObjectStoreWriter.dfToId.remove(df)
    ObjectRefHolder.removeQueue(uuid)
  }

}

object ObjectStoreWriter {
  val dfToId = new mutable.HashMap[DataFrame, UUID]()
  var driverAgent: RayDPDriverAgent = _
  var driverAgentUrl: String = _
  var address: Array[Byte] = null

  def connectToRay(): Unit = {
    if (!Ray.isInitialized) {
      Ray.init()
      // restore log level to WARN since it's inside Spark driver
      SparkContext.getOrCreate().setLogLevel("WARN")
      driverAgent = new RayDPDriverAgent()
      driverAgentUrl = driverAgent.getDriverAgentEndpointUrl
    }
  }

  def getAddress(): Array[Byte] = {
    if (address == null) {
      val objectRef = Ray.put(1)
      val objectRefImpl = RayDPUtils.convert(objectRef)
      val objectId = objectRefImpl.getId
      val runtime = Ray.internal.asInstanceOf[AbstractRayRuntime]
      address = runtime.getObjectStore.getOwnershipInfo(objectId)
    }
    address
  }

  def toArrowSchema(df: DataFrame): Schema = {
    val conf = df.queryExecution.sparkSession.sessionState.conf
    val timeZoneId = conf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE)
    ArrowUtils.toArrowSchema(df.schema, timeZoneId)
  }

  def fromSparkRDD(df: DataFrame, storageLevel: StorageLevel): Array[Array[Byte]] = {
    if (!Ray.isInitialized) {
      throw new RayDPException(
        "Not yet connected to Ray! Please set fault_tolerant_mode=True when starting RayDP.")
    }
    val uuid = dfToId.getOrElseUpdate(df, UUID.randomUUID())
    val queue = ObjectRefHolder.getQueue(uuid)
    val rdd = df.toArrowBatchRdd
    rdd.persist(storageLevel)
    rdd.count()
    var executorIds = df.sqlContext.sparkContext.getExecutorIds.toArray
    val numExecutors = executorIds.length
    val appMasterHandle = Ray.getActor(RayAppMaster.ACTOR_NAME)
                             .get.asInstanceOf[ActorHandle[RayAppMaster]]
    val restartedExecutors = RayAppMasterUtils.getRestartedExecutors(appMasterHandle)
    // Check if there is any restarted executors
    if (!restartedExecutors.isEmpty) {
      // If present, need to use the old id to find ray actors
      for (i <- 0 until numExecutors) {
        if (restartedExecutors.containsKey(executorIds(i))) {
          val oldId = restartedExecutors.get(executorIds(i))
          executorIds(i) = oldId
        }
      }
    }
    val schema = ObjectStoreWriter.toArrowSchema(df).toJson
    val numPartitions = rdd.getNumPartitions
    val results = new Array[Array[Byte]](numPartitions)
    val refs = new Array[ObjectRef[Array[Byte]]](numPartitions)
    val handles = executorIds.map {id =>
      Ray.getActor("raydp-executor-" + id)
         .get
         .asInstanceOf[ActorHandle[RayDPExecutor]]
    }
    val handlesMap = (executorIds zip handles).toMap
    val locations = RayExecutorUtils.getBlockLocations(
        handles(0), rdd.id, numPartitions)
    for (i <- 0 until numPartitions) {
      // TODO use getPreferredLocs, but we don't have a host ip to actor table now
      refs(i) = RayExecutorUtils.getRDDPartition(
          handlesMap(locations(i)), rdd.id, i, schema, driverAgentUrl)
      queue.add(refs(i))
    }
    for (i <- 0 until numPartitions) {
      results(i) = RayDPUtils.convert(refs(i)).getId.getBytes
    }
    results
  }

}

object ObjectRefHolder {
  type Queue = ConcurrentLinkedQueue[ObjectRef[Array[Byte]]]
  private val dfToQueue = new ConcurrentHashMap[UUID, Queue]()

  def getQueue(df: UUID): Queue = {
    dfToQueue.computeIfAbsent(df, new JFunction[UUID, Queue] {
      override def apply(v1: UUID): Queue = {
        new Queue()
      }
    })
  }

  @inline
  def checkQueueExists(df: UUID): Queue = {
    val queue = dfToQueue.get(df)
    if (queue == null) {
      throw new RuntimeException("The DataFrame does not exist")
    }
    queue
  }

  def getQueueSize(df: UUID): Int = {
    val queue = checkQueueExists(df)
    queue.size()
  }

  def getRandom(df: UUID): Array[Byte] = {
    val queue = checkQueueExists(df)
    val ref = RayDPUtils.convert(queue.peek())
    ref.get()
  }

  def removeQueue(df: UUID): Unit = {
    dfToQueue.remove(df)
  }

  def clean(): Unit = {
    dfToQueue.clear()
  }
}
