package latis.time

import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone
import java.text.ParseException

/**
 * TimeFormat support that is thread safe and assumes GMT time zone.
 */
class TimeFormat(format: String) {

  private val sdf: SimpleDateFormat = {
    val sdf = try new SimpleDateFormat(format) catch {
      case e: Exception => throw new IllegalArgumentException(
        s"Could not parse '$format' as a time format: ${e.getMessage}")
    }
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    sdf
  }

  def format(millis: Long): String = this.synchronized {
    sdf.format(new Date(millis))
  }

  def parse(string: String): Long = this.synchronized {
    try {
      sdf.parse(string).getTime
    } catch {
      case e: ParseException => throw new IllegalArgumentException("Unable to parse time string (" + string + ") with the format " + format)
    }
  }
  
  /**
   * Sets the 100-year period 2-digit years will be interpreted as 
   * being in to begin on the Time.
   */
  def setCenturyStart(date: Time): TimeFormat = {
    sdf.set2DigitYearStart(new Date(date.getJavaTime))
    this
  }

  override def toString = format
}

object TimeFormat {

  val DATE = TimeFormat("yyyy-MM-dd")
  val DATETIME = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val ISO = DATETIME

  def apply(format: String): TimeFormat = new TimeFormat(format)

  def fromIsoValue(s: String): TimeFormat = {
    val (date, delim, time) = splitDateTime(s)
    val dateFormat = getDateFormatString(date)
    val timeFormat = getTimeFormatString(time)
    val format = dateFormat + delim + timeFormat
    new TimeFormat(format)
  }

  private def splitDateTime(s: String): (String, String, String) = {
    //need delimiter so we can reconstruct
    val delim = if (s.contains("T")) "'T'" //need to put literal in quotes for TimeFormat
    else if (s.contains(" ")) " "
    else ""
    //splitting on 'T' or space
    val ss = s.split("[T ]")
    if (ss.length == 2) (ss(0), delim, ss(1)) // date, time pair
    else (ss(0), delim, "") //date only
  }

  private def getDateFormatString(s: String): String = s.length match {
    case 4 => "yyyy"
    case 6 => "yyMMdd" //Note, yyyyMM not iso compliant
    case 7 => {
      if (s.contains("-")) "yyyy-MM"
      else "yyyyDDD"
    }
    case 8 => {
      if (s.contains("-")) "yyyy-DDD"
      else "yyyyMMdd"
    }
    case 10 => "yyyy-MM-dd"
    case _ => throw new IllegalArgumentException("Failed to determine a date format for " + s)
  }

  private def getTimeFormatString(s: String): String = {
    /*
     * TODO: handle time zone:
     * <time>Z
     * <time>+/-hh:mm
     * <time>+/-hhmm 
     * <time>+/-hh
     */
    //make sure we match times with "Z" for now
    val length = s.indexOf("Z") match {
      case n: Int if (n != -1) => n
      case _ => s.length
    }
    
    length match {
      case 0 => ""
      case 2 => "HH"
      case 4 => "HHmm"
      case 5 => "HH:mm"
      case 6 => "HHmmss"
      case 8 => "HH:mm:ss"
      case 12 => "HH:mm:ss.SSS"
      //TODO: investigate how forgiving the number of decimal points are.
      //  appears that 0.0010 is taken to be 10 ms !?
      case _ => throw new IllegalArgumentException("Failed to determine a time format for " + s)
      //TODO: fraction of sec? or just milliseconds?
      //TODO: allow fraction of any trailing value (e.g. year + fraction of year). Not iso but handy
    }
  }
}