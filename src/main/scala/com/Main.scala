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
      .mapAsync(1)(readFromFile(_).filter(_.crimeId.isDefined).runWith(Sink.seq))
      .map(_.groupBy(_.coorditanes))
      .runWith(Sink.seq)
      .map(println)
  }

  def readFromFile(fileName: String) = {
    FileIO
      .fromPath(Paths.get(fileName))
      .via(CsvParsing.lineScanner())
      .via(CsvToMap.toMap())
      .map(_.mapValues(_.utf8String))
      .map(intoModel)
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


  def readFilesNameFrom(dir: String) = {
    Path(dir).toDirectory
      .list
      .toList
      .map(_.toString())
      .filter(csvFilesOnly)
  }

  private val csvFilesOnly: String => Boolean = _.endsWith(".csv")
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

  val coorditanes: String = s"$latitude + $longitude"
}

object Calculator {
  def add(a: Int, b: Int): Int = a + b
}

