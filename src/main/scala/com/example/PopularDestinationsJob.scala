package com.example

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, dayofweek, dense_rank, from_utc_timestamp, month, udf}
import com.example.storage.Storage

object PopularDestinationsJob extends BaseSparkJob {
  val jobName = "PopularDestinationsJob"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    val lookupRdd = storage.airports.select("IATA", "Country").rdd
    val lookupCountriesMap = spark.sparkContext.broadcast(
      lookupRdd.map(x => x(0).toString).zip(lookupRdd.map(x => x(1).toString)).collectAsMap
    )
    def lookupCountries: UserDefinedFunction = udf((IATA: String) => lookupCountriesMap.value.get(IATA))
    def getSeason: UserDefinedFunction = udf((month: Int) => {
      val monthToSeason = Map(
        1 -> "Winter", 2 -> "Winter", 3 -> "Spring", 4 -> "Spring", 5 -> "Spring", 6 -> "Summer",
        7 -> "Summer", 8 -> "Summer", 9 -> "Fall", 10 -> "Fall", 11 -> "Fall", 12 -> "Winter"
      )
      monthToSeason.get(month)
    })
    def getDayOfWeek: UserDefinedFunction = udf((day: Int) => {
      val days = Map(
        1 -> "Sunday", 2 -> "Monday", 3 -> "Tuesday", 4 -> "Wednesday", 5 -> "Thursday", 6 -> "Friday", 7 -> "Saturday"
      )
      days.get(day)
    })

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
      .withColumn("departureDateAmsterdam", from_utc_timestamp(col("departureDate"), "Europe/Amsterdam"))
      .withColumn("departureDay", getDayOfWeek(dayofweek(col("departureDateAmsterdam"))))
      .withColumn("departureSeason", getSeason(month(col("departureDateAmsterdam"))))
      .groupBy("destinationCountry", "departureSeason", "departureDay")
      .count()
      .sort(col("count").desc)
      .show(numRows = 100, truncate = false)
  }
}