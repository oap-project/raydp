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

import io.ray.api.Ray
import io.ray.runtime.config.RayConfig
import org.json4s._
import org.json4s.jackson.JsonMethods._

class AppMasterJavaBridge {
  private var instance: RayAppMaster = null

  def setProperties(properties: String): Unit = {
    implicit val formats = org.json4s.DefaultFormats
    val parsed = parse(properties).extract[Map[String, String]]
    parsed.foreach{ case (key, value) =>
      System.setProperty(key, value)
    }
    // Use the same session dir as the python side
    RayConfig.getInstance().setSessionDir(System.getProperty("ray.session-dir"))
  }

  def startUpAppMaster(extra_cp: String): Unit = {
    if (instance == null) {
      // init ray, we should set the config by java properties
      Ray.init()
      instance = new RayAppMaster(extra_cp)
    }
  }

  def getMasterUrl(): String = {
    if (instance == null) {
      throw new RuntimeException("You should create the RayAppMaster instance first")
    }
    instance.getMasterUrl()
  }

  def stop(): Unit = {
    if (instance != null) {
      instance.stop()
      instance = null
    }
  }
}
