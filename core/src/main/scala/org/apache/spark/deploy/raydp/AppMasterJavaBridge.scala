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
