package latis.time

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.Date
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

/**
 * TimeFormat support that is thread safe and assumes GMT time zone.
 * Delegates to joda-time DateTimeFormat.
 */
class TimeFormat(format: String) {
  
  val formatter = DateTimeFormat.forPattern(format).withZoneUTC
  
  def format(millis: Long): String = {
    formatter.print(millis)
  }
    
  def parse(string: String): Long = {
    formatter.parseMillis(string.trim)
  }

  override def toString = format
}

object TimeFormat {
  
  val DATE     = TimeFormat("yyyy-MM-dd")
  val DATETIME = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val ISO      = DATETIME //TODO: consider ISODateTimeFormat.dateTime, but has ZZ
  
  def apply(format: String): TimeFormat = new TimeFormat(format)
  
}