package com.example

import java.sql.Timestamp
import com.example.storage.LocalStorage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object Notebook {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkExample").getOrCreate()
    val startDate = Timestamp.valueOf("2019-01-01 00:00:00")
    val endDate = Timestamp.valueOf("2019-05-31 14:55:00")
    val config = UsageConfig(startDate, endDate)
    val storage = new LocalStorage(spark, config.bookingsURI, config.airportsURI)

    storage.bookings
      .filter(col("departureDate") >= config.startDate && col("departureDate") <= config.endDate)
      .sort(col("departureDate").asc)
      .show(100, truncate = false)
  }
}
