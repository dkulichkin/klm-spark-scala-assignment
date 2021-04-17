package com.example
import com.example.storage.Storage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object PopularDestinationsJob extends BaseSparkJob {
  val jobName = "PopularDestinationsJob"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    storage.bookings
      .filter(col("timestamp") >= config.startDate && col("timestamp") <= config.endDate)
      .filter(col("operatingAirline") === "KL")
      .alias("bookings")
      .join(storage.airports, col("originAirport") === col("IATA"))
      .withColumnRenamed("Country", "originCountry")
      .filter(col("originCountry") === "Netherlands")
      .select("bookings.*", "originCountry")
      .show(100, truncate = false)
  }
}