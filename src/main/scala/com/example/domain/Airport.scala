package com.example.domain

import org.apache.spark.sql.types.{IntegerType, DoubleType, StringType, StructField, StructType}

case class Airport(ID: String,
                   Name: String,
                   City: String,
                   Country: String,
                   IATA: String,
                   ICAO: String,
                   Latitude: Double,
                   Longitude: Double,
                   Altitude: Int,
                   Timezone: Int,
                   DST: String,
                   TimezoneOlson: String,
                   Type: String,
                   Source: String
                  )

object Airport {
  val SCHEMA = new StructType(Array(
    StructField("ID", StringType, nullable = false),
    StructField("Name", StringType, nullable = false),
    StructField("City", StringType, nullable = false),
    StructField("Country", StringType, nullable = false),
    StructField("IATA", StringType, nullable = true),
    StructField("ICAO", StringType, nullable = true),
    StructField("Latitude", DoubleType, nullable = false),
    StructField("Longitude", DoubleType, nullable = false),
    StructField("Altitude", IntegerType, nullable = false),
    StructField("Timezone", IntegerType, nullable = false),
    StructField("DST", StringType, nullable = false),
    StructField("TimezoneOlson", StringType, nullable = false),
    StructField("Type", StringType, nullable = false),
    StructField("Source", StringType, nullable = false),
  ))
}