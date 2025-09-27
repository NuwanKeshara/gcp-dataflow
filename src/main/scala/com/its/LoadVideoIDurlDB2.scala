package com.its

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LoadVideoIDurlDB2 extends App{

  val conf = new SparkConf()
    .setAppName("readParquet")
    .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

  val spark = SparkSession.builder
    .master("local[*]").config(conf).getOrCreate()

  val GCSLandingPath = "data\\USvideos.csv"

  val videoUrlsDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\\")
    .option("multiline", "true")
    .csv(GCSLandingPath)
    .select("video_id", "thumbnail_link")
    .withColumnRenamed("thumbnail_link", "url")

  val jdbcUrl = "jdbc:postgresql://localhost:5433/postgres"
  val connectionProperties = new java.util.Properties()
  connectionProperties.setProperty("user", "root")
  connectionProperties.setProperty("password", "password")
  connectionProperties.setProperty("driver", "org.postgresql.Driver")

  videoUrlsDF.dropDuplicates.write
    .mode("append")
    .jdbc(jdbcUrl, "public.YVIDEO_API_URL", connectionProperties)

  spark.stop()


}