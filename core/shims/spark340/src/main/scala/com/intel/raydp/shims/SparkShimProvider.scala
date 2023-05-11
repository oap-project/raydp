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

package com.intel.raydp.shims.spark340

import com.intel.raydp.shims.{SparkShims, SparkShimDescriptor}

object SparkShimProvider {
  val SPARK340_DESCRIPTOR = SparkShimDescriptor(3, 4, 0)
  val DESCRIPTOR_STRINGS = Seq(s"$SPARK340_DESCRIPTOR")
  val DESCRIPTOR = SPARK340_DESCRIPTOR
}

class SparkShimProvider extends com.intel.raydp.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark340Shims()
  }

  def matches(version: String): Boolean = {
    SparkShimProvider.DESCRIPTOR_STRINGS.contains(version)
  }
}
