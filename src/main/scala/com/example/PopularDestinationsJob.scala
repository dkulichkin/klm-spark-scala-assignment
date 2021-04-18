package com.example

import java.sql.Timestamp
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, dayofweek, dense_rank, from_utc_timestamp, month, udf}

import com.example.storage.Storage
import com.example.domain._

object PopularDestinationsJob extends BaseSparkJob {
  val jobName = "PopularDestinationsJob"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    val lookupRdd = storage.airports.select("IATA", "Country").rdd
    val lookupCountriesMap = spark.sparkContext.broadcast(
      lookupRdd.map(x => x(0).toString).zip(lookupRdd.map(x => x(1).toString)).collectAsMap
    )

    storage.bookings
      .transform(applyFilterByDepartureDate(config.startDate, config.endDate))
      .transform(applyFilterByAirlines("KL"))
      .transform(applyFilterByConfirmedOnly)
      .transform(resolveCountriesByIATA(lookupCountriesMap.value))
      .transform(applyFilterByOriginCountry("Netherlands"))
      .transform(createDepartureDateLocalTimeColumn("Europe/Amsterdam"))
      .transform(createDepartureDayColumn)
      .transform(createDepartureSeasonColumn)
      .transform(groupForReport)
      .show(numRows = 100, truncate = false)
  }

  def applyFilterByDepartureDate(startDate: Timestamp, endDate: Timestamp)(bookings: Dataset[Booking]): Dataset[Booking] = {
    bookings.filter(col("departureDate") >= startDate && col("departureDate") <= endDate)
  }

  def applyFilterByAirlines(airlinesCode: String)(bookings: Dataset[Booking]): Dataset[Booking] = {
    bookings.filter(col("operatingAirline") === airlinesCode)
  }

  def applyFilterByConfirmedOnly(bookings: Dataset[Booking]): DataFrame = {
    val windowSpec = Window
      .partitionBy("uci", "originAirport", "destinationAirport")
      .orderBy(col("departureDate").desc)

    bookings
      .withColumn("rank", dense_rank().over(windowSpec))
      .filter(col("rank") === 1 && col("bookingStatus") === "CONFIRMED")
  }

  def resolveCountriesByIATA(lookupCountriesMap: scala.collection.Map[String, String])(bookings: DataFrame): DataFrame = {
    def lookupCountries: UserDefinedFunction = udf((IATA: String) => lookupCountriesMap.get(IATA))

    bookings
      .withColumn("originCountry", lookupCountries(col("originAirport")))
      .withColumn("destinationCountry", lookupCountries(col("destinationAirport")))
  }

  def applyFilterByOriginCountry(country: String)(bookings: DataFrame): DataFrame = {
    bookings.filter(col("originCountry") === country)
  }

  def createDepartureDateLocalTimeColumn(tz: String)(bookings: DataFrame): DataFrame = {
    bookings.withColumn("departureDateLocal", from_utc_timestamp(col("departureDate"), tz))
  }

  def createDepartureDayColumn(bookings: DataFrame): DataFrame = {
    def getDayOfWeek: UserDefinedFunction = udf((day: Int) => {
      val days = Map(1 -> "Sunday", 2 -> "Monday", 3 -> "Tuesday", 4 -> "Wednesday", 5 -> "Thursday", 6 -> "Friday", 7 -> "Saturday")
      days.get(day)
    })

    bookings.withColumn("departureDay", getDayOfWeek(dayofweek(col("departureDateLocal"))))
  }

  def createDepartureSeasonColumn(bookings: DataFrame): DataFrame = {
    def getSeason: UserDefinedFunction = udf((month: Int) => {
      val monthToSeason = Map(
        1 -> "Winter", 2 -> "Winter", 3 -> "Spring", 4 -> "Spring", 5 -> "Spring", 6 -> "Summer",
        7 -> "Summer", 8 -> "Summer", 9 -> "Fall", 10 -> "Fall", 11 -> "Fall", 12 -> "Winter"
      )
      monthToSeason.get(month)
    })
    bookings.withColumn("departureSeason", getSeason(month(col("departureDateLocal"))))
  }

  def groupForReport(bookings: DataFrame): DataFrame = {
    bookings
      .groupBy("destinationCountry", "departureSeason", "departureDay")
      .count()
      .sort(col("count").desc)
  }
}