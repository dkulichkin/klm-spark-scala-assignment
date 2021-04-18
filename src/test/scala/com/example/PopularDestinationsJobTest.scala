package com.example

import com.example.domain.Booking
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

import java.sql.Timestamp


class PopularDestinationsJobTest extends FunSuite with DatasetSuiteBase {
  test("applyFilterByDepartureDate") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = List(
      Booking(
        timestamp = Timestamp.valueOf("2019-03-17 13:47:26"), uci = "uci1", age = 16, passengerType = "Adt", bookingStatus = "CONFIRMED",
        operatingAirline = "KL", originAirport = "AMS", destinationAirport = "LIM",
        departureDate = Timestamp.valueOf("2019-03-20 13:00:00"),
        arrivalDate = Timestamp.valueOf("2019-03-21 09:00:00"),
      ),
      Booking(
        timestamp = Timestamp.valueOf("2019-04-17 13:47:26"), uci = "uci1", age = 16, passengerType = "Adt", bookingStatus = "CONFIRMED",
        operatingAirline = "KL", originAirport = "FOO", destinationAirport = "TRF",
        departureDate = Timestamp.valueOf("2019-04-20 13:00:00"),
        arrivalDate = Timestamp.valueOf("2019-04-21 09:00:00"),
      ),
      Booking(
        timestamp = Timestamp.valueOf("2019-05-17 13:47:26"), uci = "uci1", age = 16, passengerType = "Adt", bookingStatus = "CONFIRMED",
        operatingAirline = "KL", originAirport = "BAR", destinationAirport = "BAZ",
        departureDate = Timestamp.valueOf("2019-05-20 13:00:00"),
        arrivalDate = Timestamp.valueOf("2019-05-21 09:00:00"),
      )
    )

    val inputDf: Dataset[Booking] = sc.parallelize(input).toDS
    val outputDf: Dataset[Booking] = sc.parallelize(input.take(2)).toDS

    val startDate = Timestamp.valueOf("2019-03-01 00:00:00")
    val endDate = Timestamp.valueOf("2019-05-01 00:00:00")
    assertDatasetEquals(
      inputDf.transform(PopularDestinationsJob.applyFilterByDepartureDate(startDate, endDate)),
      outputDf
    )
  }
}
