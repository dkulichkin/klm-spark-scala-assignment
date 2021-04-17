package com.example.domain

import java.sql.Timestamp
import scala.collection.immutable.ListMap

case class Booking(timestamp: Timestamp,
                   uci: String,
                   age: Int,
                   passengerType: String,
                   bookingStatus: String,
                   operatingAirline: String,
                   originAirport: String,
                   destinationAirport: String,
                   departureDate: Timestamp,
                   arrivalDate: Timestamp,
                  )

object Booking {
  def columnsTranslatorMap: ListMap[String, String] = ListMap(
    "timestamp" -> "timestamp",
    "passengersList.uci" -> "uci",
    "passengersList.age" -> "age",
    "passengersList.passengerType" -> "passengerType",
    "productsList.bookingStatus" -> "bookingStatus",
    "productsList.flight.operatingAirline" -> "operatingAirline",
    "productsList.flight.originAirport" -> "originAirport",
    "productsList.flight.destinationAirport" -> "destinationAirport",
    "productsList.flight.departureDate" -> "departureDate",
    "productsList.flight.arrivalDate" -> "arrivalDate",
  )
}
