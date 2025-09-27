package com.its.framework.config

import org.apache.spark.SparkConf

object SparkConfig {
  def getSparkConf: SparkConf = {
    new SparkConf()
      .setAppName("gcp-dataflow")
      .set("spark.sql.shuffle.partitions", "8")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "2g")
  }
}