package latis.reader.tsml

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import latis.data.Data
import latis.data.value.StringValue
import latis.dm.Scalar
import latis.reader.tsml.ml.Tsml
import latis.time._

/**
 * Experimental version of UrlListGenerator that uses formatted (text) time
 * and the java.time API so we can define a step size (e.g. month) that
 * isn't otherwise constant in SI units.
 * This was set aside in favor of latis-swp GoesMonthlyUrlListGenerator.
 */
class UrlListGenerator2(tsml: Tsml) extends IterativeAdapter2[LocalDateTime](tsml) {

  /**
   * Finds the first time variable.
   */
  private lazy val timeVar: Scalar = getOrigScalars.find(_.hasName("time")).getOrElse {
    throw new RuntimeException("No time variable defined")
  }

  /**
   * Gets the given time property from the time variable metadata
   * and return it as a LocalDataTime.
   */
  private def getDateTime(timeProperty: String): Option[LocalDateTime] =
    timeVar.getMetadata(timeProperty).map { startTime =>
      val ms = Time.isoToJava(startTime) //TODO: handle invalid format
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(ms),
        ZoneId.of("Z")
      )
    }

  /**
   * Parses the start time or throws an error if not defined.
   */
  private lazy val startDateTime: LocalDateTime =
    getDateTime("startTime").getOrElse {
      throw new RuntimeException("startTime not defined")
    }

  /**
   * Parses the end time. Defaults to now if not defined.
   */
  private lazy val endDateTime: LocalDateTime =
    getDateTime("endTime").getOrElse(LocalDateTime.now())

  /**
   * Parses the cadence as a number of ChronoUnits.
   */
  private lazy val (step, unit): (Int, ChronoUnit) =
    timeVar.getMetadata("cadence").map { cadence =>
      cadence.split("\\s+").toList match {
        case s1 :: s2 :: Nil => (
          s1.toInt,
          ChronoUnit.valueOf(s2)
        )
        case _ => throw new RuntimeException(s"Invalid cadence: $cadence")
      }
    }.getOrElse {
      throw new RuntimeException("cadence not defined")
    }

  /**
   * Gets a formatter for the time variable units.
   */
  lazy val formatter: DateTimeFormatter = timeVar.getMetadata("units").map {
    DateTimeFormatter.ofPattern(_)
  }.getOrElse {
    throw new RuntimeException("Time units not defined.")
  }

  /**
   * Gets the URL pattern with time encoding from the url variable.
   */
  private lazy val pattern: String = tsml.getVariableAttribute("url", "pattern") //TODO: error

  /**
   * Creates an Iterator of times as java.time.LocalDateTime
   * incrementing by the cadence.
   */
  def getRecordIterator: Iterator[LocalDateTime] = new Iterator[LocalDateTime] {
    private var _next: LocalDateTime = startDateTime

    def next(): LocalDateTime = {
      val current = _next
      _next = _next.plus(step, unit)
      current
    }

    def hasNext: Boolean = endDateTime.compareTo(_next) > 0
  }

  /**
   * Creates 'time' and 'url' data for each record (i.e. time sample).
   */
  def parseRecord(rec: LocalDateTime): Option[Map[String, Data]] = {
    val time: String = rec.format(formatter)
    val url: String = pattern.format(rec)
    val map = Map("time" -> StringValue(time), "url" -> StringValue(url))
    Some(map)
  }

  /** No resources to release. */
  def close: Unit = {}
}
