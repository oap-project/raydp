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

package org.apache.spark.deploy.raydp

import java.util.Map

import scala.collection.JavaConverters._

import io.ray.api.{ActorHandle, Ray}

import org.apache.spark.raydp.SparkOnRayConfigs

class AppMasterJavaBridge {
  private var handle: ActorHandle[RayAppMaster] = null

  def startUpAppMaster(extra_cp: String, sparkProps: Map[String, Any]): Unit = {
    if (handle == null) {
      // init ray, we should set the config by java properties
      Ray.init()
      val name = RayAppMaster.ACTOR_NAME
      val sparkJvmOptions = sparkProps.asScala.toMap.filter(
        e => {
          !SparkOnRayConfigs.SPARK_DRIVER_EXTRA_JAVA_OPTIONS.equals(e._1) &&
            !SparkOnRayConfigs.SPARK_APP_MASTER_EXTRA_JAVA_OPTIONS.equals(e._1)
        })
        .map {
          case (k, v) =>
            if (!SparkOnRayConfigs.SPARK_JAVAAGENT.equals(k)) {
              "-D" + k + "=" + v
            } else {
              "-javaagent:" + v
            }
        }.toBuffer

      // Add raw JVM options from spark.ray.raydp_app_master.extraJavaOptions
      // (e.g., --add-opens for JDK 17)
      val appMasterExtraJavaOptions =
        sparkProps.get(SparkOnRayConfigs.SPARK_APP_MASTER_EXTRA_JAVA_OPTIONS)
      if (appMasterExtraJavaOptions != null) {
        val opts = appMasterExtraJavaOptions.toString.trim
        if (opts.nonEmpty) {
          sparkJvmOptions ++= opts.split("\\s+").toSeq
        }
      }

      val appMasterResources = sparkProps.asScala.filter {
        case (k, v) => k.startsWith(SparkOnRayConfigs.SPARK_MASTER_ACTOR_RESOURCE_PREFIX)
      }.map{ case (k, v) => k->double2Double(v.toString.toDouble) }.asJava

      handle = RayAppMasterUtils.createAppMaster(
          extra_cp, name,
          (sparkJvmOptions ++ Seq(SparkOnRayConfigs.RAYDP_LOGFILE_PREFIX_CFG)).asJava,
          appMasterResources)
    }
  }

  def getMasterUrl(): String = {
    if (handle == null) {
      throw new RuntimeException("You should create the RayAppMaster handle first")
    }
    RayAppMasterUtils.getMasterUrl(handle)
  }

  def stop(): Unit = {
    if (handle != null) {
      RayAppMasterUtils.stopAppMaster(handle)
      Ray.shutdown()
      handle = null
    }
  }
}
