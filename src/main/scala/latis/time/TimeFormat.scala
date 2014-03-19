package latis.time

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.Date

/**
 * TimeFormat support that is thread safe and assumes GMT time zone.
 */
class TimeFormat(format: String) {
  
  private val sdf: SimpleDateFormat = {
    val sdf = new SimpleDateFormat(format)
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    sdf
  }
  
  def format(millis: Long): String = this.synchronized {
    sdf.format(new Date(millis))
  }
  
  def parse(string: String): Long = this.synchronized {
    sdf.parse(string).getTime
  }

  override def toString = format
}

object TimeFormat {
  
  val DATE     = TimeFormat("yyyy-MM-dd")
  val DATETIME = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val ISO      = DATETIME 
  
  def apply(format: String): TimeFormat = new TimeFormat(format)
  
}