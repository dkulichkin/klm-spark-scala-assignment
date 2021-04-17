package com.example

import com.example.storage.{LocalStorage, Storage}
import org.apache.spark.sql.SparkSession

trait BaseSparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(jobName).getOrCreate()
    new UsageOptionParser().parse(args, UsageConfig()) match {
      case Some(config) => run(spark, config, new LocalStorage(spark, config.bookingsURI, config.airportsURI))
      case None => throw new IllegalArgumentException("Arguments provided to job are not valid")
    }
  }

  def run(spark: SparkSession, config: UsageConfig, storage: Storage)

  val jobName: String
}
