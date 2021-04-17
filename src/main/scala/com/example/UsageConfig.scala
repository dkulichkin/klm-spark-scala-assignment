package com.example

import java.sql.Timestamp
import scopt.OptionParser

case class UsageConfig(startDate: Timestamp = new Timestamp(System.currentTimeMillis()),
                       endDate: Timestamp = new Timestamp(System.currentTimeMillis()),
                       bookingsURI: String = "data/bookings/booking.json",
                       airportsURI: String = "data/airports/airports.dat"
                      )

class UsageOptionParser extends OptionParser[UsageConfig]("job config") {

  val format = new java.text.SimpleDateFormat("dd-MM-yyyy")

  head("scopt", "4.x")

  opt[String]( "startDate").abbr("sd").required
    .action((value, config) => config.copy(startDate = new Timestamp(format.parse(value).getTime)))
    .text("Start date (dd-MM-yyyy)")

  opt[String]( "endDate").abbr("ed").required
    .action((value, config) => config.copy(endDate = new Timestamp(format.parse(value).getTime)))
    .text("End date (dd-MM-yyyy)")

  opt[String]( "bookingsURI").optional
    .action((value, config) => config.copy(bookingsURI = value))
    .text("Bookings URI (default: data/bookings/booking.json)")

  opt[String]( "airportsURI").optional
    .action((value, config) => config.copy(airportsURI = value))
    .text("Airports URI (default: data/airports/airports.dat)")
}
