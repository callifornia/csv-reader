package com

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Sink, Source}

import scala.concurrent.Future
import scala.reflect.io.Path

object Main {

  val ABSOLUTE_PATH_INTO_DIRECTORY_WITH_FILES = "/Users/hryhorii/projects/csv-reader/src/main/resources/crimes"

  implicit val system = ActorSystem("foo")
  implicit val ec = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()


  def main(args: Array[String]): Unit = {
    Source(readFilesNameFrom(args(0)))
      .mapAsync(1)(readFromFile(_)
        .filter(withCrimeId)
        .runWith(Sink.seq))
      .runWith(Sink.seq)
      .map(_.flatten.distinct.groupBy(_.coorditanes).map(intoRecordsStatistic).toSeq.sortBy(_.records.size).takeRight(5).reverse)
      .foreach(_.foreach(println))

  }

  def readFromFile(fileName: String) = {
    FileIO
      .fromPath(Paths.get(fileName))
      .via(CsvParsing.lineScanner())
      .via(CsvToMap.toMap())
      .map(_.mapValues(_.utf8String))
      .map(intoModel)
  }

  val intoRecordsStatistic: PartialFunction[(String, Seq[CrimeRecord]), CrimeRecordsStatistic] = {
    case (latlon, records) => CrimeRecordsStatistic(latlon, records)
  }

  def intoModel(map: Map[String, String]): CrimeRecord = CrimeRecord(
    map.get("Location"),
    map.get("Latitude"),
    map.get("Longitude"),
    map.get("Month"),
    map.get("Falls within"),
    map.get("LSOA name"),
    map.get("Last outcome category"),
    map.get("Context"),
    map.get("Reported by"),
    map.get("Crime type"),
    map.get("Crime ID").filter(_.nonEmpty),
    map.get("LSOA code"))

  val withCrimeId: CrimeRecord => Boolean = _.crimeId.isDefined

  def readFilesNameFrom(dir: String) = {
    Path(dir).toDirectory
      .list
      .toList
      .map(_.toString())
      .filter(csvFilesOnly)
  }

  val csvFilesOnly: String => Boolean = _.endsWith(".csv")
}



case class CrimeRecordsStatistic(location: String,
                                 records: Seq[CrimeRecord]) {


  override def toString(): String = {
    s"""
       | $location: ${records.size}
       | ${records.map(_.crimeType).distinct.mkString("\n")}
       |
     """.stripMargin
  }
}

case class CrimeRecord(location: Option[String],
                       latitude: Option[String],
                       longitude: Option[String],
                       month: Option[String],
                       fallsWithin: Option[String],
                       lsoaName: Option[String],
                       lastOutcomeCategory: Option[String],
                       context: Option[String],
                       reportedBy: Option[String],
                       crimeType: Option[String],
                       crimeId: Option[String],
                       lsoaCode: Option[String]) {

  val coorditanes: String = s"($latitude, $longitude)"
}

// Output
//(Some(), Some()): 9504
//Some(Burglary)
//Some(Criminal damage and arson)
//Some(Drugs)
//Some(Other theft)
//Some(Vehicle crime)
//Some(Violence and sexual offences)
//Some(Other crime)
//Some(Bicycle theft)
//Some(Possession of weapons)
//Some(Public order)
//Some(Robbery)
//Some(Shoplifting)
//Some(Theft from the person)
//
//
//
//(Some(51.547097), Some(-0.009573)): 246
//Some(Bicycle theft)
//Some(Burglary)
//Some(Criminal damage and arson)
//Some(Drugs)
//Some(Other theft)
//Some(Possession of weapons)
//Some(Public order)
//Some(Robbery)
//Some(Shoplifting)
//Some(Theft from the person)
//Some(Vehicle crime)
//Some(Violence and sexual offences)
//
//
//
//(Some(53.482225), Some(-2.238637)): 147
//Some(Bicycle theft)
//Some(Burglary)
//Some(Criminal damage and arson)
//Some(Drugs)
//Some(Other theft)
//Some(Public order)
//Some(Robbery)
//Some(Shoplifting)
//Some(Theft from the person)
//Some(Vehicle crime)
//Some(Violence and sexual offences)
//
//
//
//(Some(51.509369), Some(-0.170573)): 143
//Some(Criminal damage and arson)
//Some(Drugs)
//Some(Other theft)
//Some(Possession of weapons)
//Some(Public order)
//Some(Robbery)
//Some(Theft from the person)
//Some(Violence and sexual offences)
//Some(Other crime)
//
//
//
//(Some(51.514868), Some(-0.153101)): 106
//Some(Bicycle theft)
//Some(Criminal damage and arson)
//Some(Other theft)
//Some(Possession of weapons)
//Some(Public order)
//Some(Robbery)
//Some(Shoplifting)
//Some(Theft from the person)
//Some(Vehicle crime)
//Some(Violence and sexual offences)
//
//
