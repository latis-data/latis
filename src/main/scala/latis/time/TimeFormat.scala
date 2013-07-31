package latis.time

import java.text.SimpleDateFormat
import java.util.TimeZone

/**
 * Handy subclass to enumerate our favorite date formats.
 * The default time zone will be GMT.
 */
class TimeFormat(format: String) extends SimpleDateFormat(format) {

  this.setTimeZone(TimeZone.getTimeZone("GMT")); //default to GMT

  override def toString = format
}

object TimeFormat {
  val DATE     = new TimeFormat("yyyy-MM-dd");
  val DATETIME = new TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
  
  def apply(format: String): TimeFormat = new TimeFormat(format)
}