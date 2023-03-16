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
import org.apache.spark.raydp.RayDPConstants

class AppMasterJavaBridge {
  private var handle: ActorHandle[RayAppMaster] = null

  def startUpAppMaster(extra_cp: String, sparkProps: Map[String, String]): Unit = {
    if (handle == null) {
      // init ray, we should set the config by java properties
      Ray.init()
      val name = RayAppMaster.ACTOR_NAME
      val sparkJvmOptions = sparkProps.asScala.filter(
        e => !RayDPConstants.SPARK_DRIVER_EXTRA_JAVA_OPTIONS.equals(e._1))
        .map  {
          case (k, v) =>
            if (!RayDPConstants.SPARK_JAVAAGENT.equals(k)) {
              "-D" + k + "=" + v
            } else {
              "-javaagent:" + v
            }
      }.toBuffer
      handle = RayAppMasterUtils.createAppMaster(
          extra_cp, name, sparkJvmOptions.asJava)
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
