package org.apache.spark.deploy.raydp

import java.util.{HashMap => JHashMap}

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.ray.api.Ray

import scala.collection.JavaConverters._

class AppMasterJavaBridge {
  private var instance: RayAppMaster = null

  def setProperties(properties: String): Unit = {
    val mapper = new ObjectMapper()
    val typeRef = new TypeReference[JHashMap[String, String]](){}
    var propertiesMap: JHashMap[String, String] = null
    try propertiesMap = mapper.readValue(properties, typeRef)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }

    propertiesMap.asScala.foreach { case (key, value) =>
      System.setProperty(key, value)
    }
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
