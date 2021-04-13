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

import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.file.Files

import org.apache.spark.internal.Logging
import py4j.GatewayServer

class AppMasterEntryPoint {
  private val appMaster: AppMasterJavaBridge = new AppMasterJavaBridge()
  private val shuffleServices = new RayShuffleServiceCluster()

  def getAppMasterBridge(): AppMasterJavaBridge = {
    appMaster
  }

  def getShuffleServices(): RayShuffleServiceCluster = {
    shuffleServices
  }
}

object AppMasterEntryPoint extends Logging {
  initializeLogIfNecessary(true)

  def main(args: Array[String]): Unit = {
    val server = new GatewayServer(new AppMasterEntryPoint())
    server.start()
    val boundPort: Int = server.getListeningPort()
    if (boundPort == -1) {
      logError(s"${server.getClass} failed to bind; exiting")
      System.exit(1)
    } else {
      logDebug(s"Started PythonGatewayServer on port $boundPort")
    }


    val connectionInfoPath = new File(sys.env("_RAYDP_APPMASTER_CONN_INFO_PATH"))
    val tmpPath = Files.createTempFile(connectionInfoPath.getParentFile().toPath(),
      "connection", ".info").toFile()

    val dos = new DataOutputStream(new FileOutputStream(tmpPath))
    dos.writeInt(boundPort)
    dos.close()

    if (!tmpPath.renameTo(connectionInfoPath)) {
      logError(s"Unable to write connection information to $connectionInfoPath.")
      System.exit(1)
    }

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
}
