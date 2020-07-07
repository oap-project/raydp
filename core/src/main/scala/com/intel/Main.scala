package com.intel

import io.ray.api.Ray
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.raydp.RayUtils

object Main {
  def main(args: Array[String]): Unit = {
    Ray.init()
    val appMaster = RayUtils.createAppMaster(2, 1000)
    var url = RayUtils.getMasterUrl(appMaster)
    url = url.replace("spark", "ray")
    val conf = new SparkConf().setAppName("RayAppTest").setMaster(url)
    val spark = new SparkContext(conf)
    val results = spark.range(0, 100, 2).count()
    println(results)
  }

}
