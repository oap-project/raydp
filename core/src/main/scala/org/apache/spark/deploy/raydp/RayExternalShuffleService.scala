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

import io.ray.api.Ray;
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SecurityManager}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}

class RayExternalShuffleService() extends Logging {

  private var instance: ExternalShuffleService = null

  def start() = {
    val cfg = driver.askSync[SparkAppConfig](
        RetrieveSparkAppConfig(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val conf = new SparkConf()
    val mgr = new SecurityManager(conf)
    for ((key, value) <- cfg.sparkProperties) {
      conf.set(key, value)
    }
    instance = new ExternalShuffleService(conf, mgr)
    instance.start()
  }

  def stop() = {
    if (instance != null) {
      instance.stop()
    }
    Ray.exitActor()
  }
}
