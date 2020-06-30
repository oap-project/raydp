package org.apache.spark.deploy.raydp

import org.apache.spark.resource.ResourceRequirement

import scala.collection.Map

private[spark] case class Command(
    arguments: Seq[String],
    environment: Map[String, String],
    classPathEntries: Seq[String],
    libraryPathEntries: Seq[String],
    javaOpts: Seq[String])

private[spark] case class ApplicationDescription(
    name: String,
    numExecutors: Int,
    coresPerExecutor: Option[Int],
    memoryPerExecutorMB: Int,
    command: Command,
    user: String = System.getProperty("user.name", "<unknown>"),
    resourceReqsPerExecutor: Seq[ResourceRequirement] = Seq.empty)