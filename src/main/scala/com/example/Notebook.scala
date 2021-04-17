package com.example

import java.sql.Timestamp
import com.example.storage.LocalStorage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object Notebook {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkExample").getOrCreate()
    val startDate = Timestamp.valueOf("2019-03-17 14:50:00")
    val endDate = Timestamp.valueOf("2019-03-17 14:55:00")
    val config = UsageConfig(startDate, endDate)
    val storage = new LocalStorage(spark, config.bookingsURI, config.airportsURI)

    storage.bookings
      .filter(col("timestamp") >= config.startDate && col("timestamp") <= config.endDate)
      .sort(col("timestamp").asc)
      .show(100, truncate = false)
  }
}
