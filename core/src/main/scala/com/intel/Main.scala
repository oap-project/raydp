package com.intel

import io.ray.api.Ray
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.raydp.RayUtils

object Main {
  def main(args: Array[String]): Unit = {
    Ray.init()
    val appMaster = RayUtils.createAppMaster(2, 100)
    var url = RayUtils.getMasterUrl(appMaster)
    println(url)
    url = url.replace("spark", "ray")
    val conf = new SparkConf()
      .setAppName("RayAppTest")
      .setMaster(url)
      .setJars(Array("file:///Users/xianyang/git/raydp/core/target/raydp-0.1-SNAPSHOT.jar"))
      .set("spark.executor.instances", "2")
      .set("spark.executor.memory", "512M")

    val spark = new SparkContext(conf)
    val results = spark.range(0, 100, 2).count()
    println(results)
  }

}
