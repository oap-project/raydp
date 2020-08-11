package org.apache.spark.sql.raydp

import java.io.ByteArrayOutputStream
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.function.{Function => JFunction}
import java.util.{List, UUID}

import io.ray.api.{ObjectRef, Ray}
import io.ray.runtime.config.RayConfig
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.raydp.RayDPUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.python.BatchIterator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class WroteRecord(nodeIp: String, numRecords: Int, objectId: Array[Byte])

class ObjectStoreWriter(@transient val df: DataFrame) extends Serializable {

  val uuid: UUID = ObjectStoreWriter.dfToId.getOrElseUpdate(df, UUID.randomUUID())

  def save(): List[WroteRecord] = {
    val conf = df.queryExecution.sparkSession.sessionState.conf
    val timeZoneId = conf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE)
    val batchSize = conf.getConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH)
    val schema = df.schema

    //
    val objectIds = df.queryExecution.toRdd.mapPartitions{iter =>
      val queue = ObjectRefHolder.getQueue(uuid)
      val nodeIp = RayConfig.getInstance().nodeIp
      // DO NOT use iter.grouped(). See BatchIterator.
      val batchIter = if (batchSize > 0) {
        new BatchIterator(iter, batchSize)
      } else {
        Iterator(iter)
      }

      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"ray object store writer", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val byteOut = new ByteArrayOutputStream()
      val results = new ArrayBuffer[WroteRecord]()

      Utils.tryWithSafeFinally {
        val arrowWriter = ArrowWriter.create(root)
        val writer = new ArrowStreamWriter(root, null, byteOut)
        writer.start()
        var numRecords: Int = 0

        while (batchIter.hasNext) {
          val nextBatch = batchIter.next()

          numRecords = 0
          while (nextBatch.hasNext) {
            numRecords += 1
            arrowWriter.write(nextBatch.next())
          }

          arrowWriter.finish()
          writer.writeBatch()
          arrowWriter.reset()


          val byteArray = byteOut.toByteArray
          val objectRef = Ray.put(byteArray)
          // add the objectRef to the objectRefHolder to avoid reference GC
          queue.add(objectRef)
          val objectRefImpl = RayDPUtils.convert(objectRef)
          // Store the ObjectId in bytes
          results += WroteRecord(nodeIp, numRecords, objectRefImpl.getId.getBytes)
          byteOut.reset()
        }
        // end writes footer to the output stream and doesn't clean any resources.
        // It could throw exception if the output stream is closed, so it should be
        // in the try block.
        writer.end()
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
    println(s"-------getRandom:${util.Arrays.toString(ref.getId.getBytes)}")
    ref.get()
  }

  def removeQueue(df: UUID): Unit = {
    dfToQueue.remove(df)
  }

  def clean(): Unit = {
    dfToQueue.clear()
  }
}
