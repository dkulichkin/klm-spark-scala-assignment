package com.example.storage

import com.example.domain._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, TimestampType}

trait Storage {
  def airports: Dataset[Airport]
  def bookings: Dataset[Booking]
}

class LocalStorage(spark: SparkSession, bookingsURI: String, airportsURI: String) extends Storage {

  import spark.implicits._

  def airports: Dataset[Airport] = spark.read.format("csv")
    .schema(Airport.SCHEMA)
    .load(airportsURI)
    .as[Airport]

  def bookings: Dataset[Booking] = spark.read.format("json")
    .load(bookingsURI)
    .withColumn("passengersList", explode($"event.DataElement.travelrecord.passengersList"))
    .withColumn("productsList", explode($"event.DataElement.travelrecord.productsList"))
    .select(
      Booking.columnsTranslatorMap.keys.toList.map(c => col(c).as(Booking.columnsTranslatorMap.getOrElse(c, c))): _*
    )
    .withColumn("age", col("age").cast(IntegerType))
    .withColumn("timestamp", col("timestamp").cast(TimestampType))
    .withColumn("departureDate", col("departureDate").cast(TimestampType))
    .withColumn("arrivalDate", col("arrivalDate").cast(TimestampType))
    .as[Booking]
}
