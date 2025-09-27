package com.its

import com.its.framework.config.SparkConfig
import org.apache.spark.sql.SparkSession
object TestSource extends App{
  
  val spark = SparkSession.builder()
    .master("local[*]")
    .config(SparkConfig.getSparkConf)
    .getOrCreate()
  
  val df = spark.read
    .option("header", "true")
    .option("InferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\\")
    .csv("data\\USvideos.csv")
  
  df.show()
}