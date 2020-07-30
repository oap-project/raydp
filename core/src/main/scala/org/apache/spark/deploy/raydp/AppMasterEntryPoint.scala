package org.apache.spark.deploy.raydp

import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.file.Files

import org.apache.spark.internal.Logging
import py4j.GatewayServer

class AppMasterEntryPoint {
  private val appMaster: AppMasterJavaBridge = new AppMasterJavaBridge()

  def getAppMasterBridge(): AppMasterJavaBridge = {
    appMaster
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
