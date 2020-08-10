package org.apache.spark.sql.raydp

import java.io.ByteArrayOutputStream
import java.util.List

import io.ray.api.{ObjectRef, Ray}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.raydp.RayDPUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.python.BatchIterator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._


class ObjectStoreWriter(val df: DataFrame) {
  import ObjectStoreWriter._

  def save(): List[Array[Byte]] = {
    val conf = df.queryExecution.sparkSession.sessionState.conf
    val timeZoneId = conf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE)
    val batchSize = conf.getConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH)
    val schema = df.schema

    //
    val objectIds = df.queryExecution.toRdd.mapPartitions{iter =>
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
      val results = new ArrayBuffer[Array[Byte]]()

      Utils.tryWithSafeFinally {
        val arrowWriter = ArrowWriter.create(root)
        val writer = new ArrowStreamWriter(root, null, byteOut)
        writer.start()

        while (batchIter.hasNext) {
          val nextBatch = batchIter.next()

          while (nextBatch.hasNext) {
            arrowWriter.write(nextBatch.next())
          }

          arrowWriter.finish()
          writer.writeBatch()
          arrowWriter.reset()


          val byteArray = byteOut.toByteArray
          val objectRef = Ray.put(byteArray)
          // add the objectRef to the objectRefHolder to avoid reference GC
          objectRefHolder += objectRef
          val objectRefImpl = RayDPUtils.convert(objectRef)
          // Store the ObjectId in bytes
          results += objectRefImpl.getId.getBytes
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

}

object ObjectStoreWriter {
  val objectRefHolder = new ArrayBuffer[ObjectRef[Array[Byte]]]()
}
