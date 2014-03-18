package latis.time

import java.util.Date
import latis.time.TimeScaleType._
import java.text.SimpleDateFormat
import scala.util.matching.Regex
import latis.units.UnitOfMeasure
import latis.util.RegEx
import org.joda.time.DateTimeUtils

class TimeScale(val epoch: Date, val unit: TimeUnit, val tsType: TimeScaleType) extends UnitOfMeasure("TODO") {
  //TODO: consider using millis for epoch instead of Date
  
  override def toString() = {
    val sb = new StringBuilder()
    sb.append(unit.name)
    sb.append(" since ")
    sb.append(TimeFormat.DATE.format(epoch.getTime))
    
    sb.toString()
  }
}

object TimeScale {
  //TODO: case objects?
  lazy val JAVA = new TimeScale(new Date(0), TimeUnit.MILLISECOND, TimeScaleType.NATIVE)
  lazy val DEFAULT = JAVA
  
//  /**
//   * Define a special case for Julian date: days since noon Jan 1, 4713 BC.
//   * Because Java's default calendar jumps from 1 BC to 1 AD, we need to use year -4712.
//   * This seems to work for the times we care about.
//   */
  //val JULIAN_DATE = TimeScale("-4712-01-01T12:00:00", TimeUnit.MILLISECOND, TimeScaleType.NATIVE)
  //TODO: not working with joda-time, try DateTimeUtils.fromJulianDate...
  lazy val JULIAN_DATE = TimeScale(new Date(DateTimeUtils.fromJulianDay(0)), TimeUnit.MILLISECOND, TimeScaleType.NATIVE)
  
  def apply(epoch: Date, unit: TimeUnit, tstype: TimeScaleType): TimeScale = {
    new TimeScale(epoch, unit, tstype)
  }
  
  def apply(epoch: String, unit: TimeUnit, tsType: TimeScaleType): TimeScale = {
    new TimeScale(new Date(TimeFormat.DATE.parse(epoch)), unit, tsType)
    //TODO: assumes yyyy-MM-dd, add support for any ISO time
    //see javax.xml.bind.DatatypeConverter.parseDateTime("2010-01-01T12:00:00Z") or Joda time
  }
  
  //TODO: other options with defaults
  
  /**
   * Make TimeScale from "unit since epoch" or time format String.
   * Assume Native TimeScaleType (no leap second consideration).
   */
  def apply(scale: String): TimeScale = {
    //TODO: encode type (e.g. UTC) in scale string?
    //TODO: allow named TimeScales, e.g. GPS
    
    //val regex = ("("+RegEx.WORD+")" + """\s+since\s+""" + "("+RegEx.TIME+")").r
    //Note, scala regex will extract group for nested ()s
    val regex = ("("+RegEx.WORD+")" + """\s+since\s+""" + """(-?[0-9]{4}-[0-9]{2}-[0-9]{2}\S*)""").r
    scale.trim match {
      case regex(unit, epoch) => TimeScale(epoch, TimeUnit.withName(unit), TimeScaleType.NATIVE)
      case _ => {
        //assume formatted time (http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html)
        TimeScale.JAVA
      }
    }
  }
}