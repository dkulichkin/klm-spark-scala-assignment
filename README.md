# Commercial Booking Analysis

Building the project:

```
sbt assembly
```

Job command line parameters:

```
Usage: job config [options]

  -sd, --startDate <value> Start date (dd-MM-yyyy)
  -ed, --endDate <value>   End date (dd-MM-yyyy)
  --bookingsURI <value>    Bookings URI (default: data/bookings/booking.json)
  --airportsURI <value>    Airports URI (default: data/airports/airports.dat)
```

Example of running with `spark-submit` locally (time scope of the year 2019):

```
spark-submit --class com.example.PopularDestinationsJob target/scala-2.12/SparkKLMExample-assembly-0.0.1.jar --startDate 01-01-2019 --endDate 31-12-2019
```

Results (top 20):

|destinationCountry  |departureSeason|departureDay|count|
|--------------------|---------------|------------|-----|
|United States       |Spring         |Monday      |641  |
|France              |Spring         |Monday      |637  |
|United Kingdom      |Spring         |Sunday      |294  |
|Italy               |Spring         |Monday      |223  |
|United States       |Spring         |Sunday      |221  |
|Portugal            |Spring         |Wednesday   |208  |
|United Kingdom      |Spring         |Monday      |160  |
|Germany             |Spring         |Monday      |151  |
|China               |Spring         |Sunday      |135  |
|United States       |Spring         |Tuesday     |115  |
|United Kingdom      |Spring         |Tuesday     |109  |
|United Kingdom      |Spring         |Saturday    |99   |
|Italy               |Spring         |Tuesday     |98   |
|China               |Spring         |Monday      |94   |
|Austria             |Spring         |Monday      |87   |
|United Kingdom      |Spring         |Thursday    |86   |
|Italy               |Spring         |Wednesday   |85   |
|United Kingdom      |Spring         |Wednesday   |81   |
|United Kingdom      |Summer         |Friday      |73   |
|United Kingdom      |Spring         |Friday      |72   |