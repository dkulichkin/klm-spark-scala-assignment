package com.example

import com.example.storage.Storage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, dense_rank, udf}

object PopularDestinationsJob extends BaseSparkJob {
  val jobName = "PopularDestinationsJob"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    val lookupRdd = storage.airports.select("IATA", "Country").rdd
    val lookupCountriesMap = spark.sparkContext.broadcast(
      lookupRdd.map(x => x(0).toString).zip(lookupRdd.map(x => x(1).toString)).collectAsMap
    )
    def lookupCountries: UserDefinedFunction = udf((IATA: String) => lookupCountriesMap.value.get(IATA))

    val windowSpec = Window
      .partitionBy("uci", "originAirport", "destinationAirport")
      .orderBy(col("departureDate").desc)

    storage.bookings
      .filter(col("departureDate") >= config.startDate && col("departureDate") <= config.endDate)
      .filter(col("operatingAirline") === "KL")
      .withColumn("rank", dense_rank().over(windowSpec))
      .filter(col("rank") === 1 && col("bookingStatus") === "CONFIRMED")
      .withColumn("originCountry", lookupCountries(col("originAirport")))
      .filter(col("originCountry") === "Netherlands")
      .withColumn("destinationCountry", lookupCountries(col("destinationAirport")))
      .show(300, truncate = false)
  }
}