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
package org.apache.spark.scheduler.cluster.raydp;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import org.apache.spark.scheduler.cluster.SchedulerBackendUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This class performs unit testing on some methods in `RayCoarseGrainedSchedulerBackend`.
 */
public class TestRayCoarseGrainedSchedulerBackend {

  // Test using the default value.
  @Test
  public void testExecutorNumberWithDefaultConfig() {
    SparkConf conf = new SparkConf();
    int executorNumber = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf, 2);
    assertEquals(2, executorNumber);
  }

  // Test using a negative value.
  @Test
  public void testExecutorNumberWithNegativeConfig() {
    SparkConf conf = new SparkConf();
    conf.set("spark.dynamicAllocation.initialExecutors", "-1");
    int executorNumber = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf, 2);
    assertEquals(2, executorNumber);
  }

  // Test using reasonable values.
  @Test
  public void testExecutorNumberWithValidConfig() {
    SparkConf conf = new SparkConf();
    conf.set("spark.executor.instances", "5");
    int executorNumber = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf, 2);
    assertEquals(5, executorNumber);
  }

  // Test using dynamic values.
  @Test
  public void testExecutorNumberWithDynamicConfig() {
    SparkConf conf = new SparkConf();
    conf.set("spark.dynamicAllocation.enabled", "true");
    conf.set("spark.dynamicAllocation.minExecutors", "3");
    int executorNumber = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf, 2);
    assertEquals(3, executorNumber);
  }
}
