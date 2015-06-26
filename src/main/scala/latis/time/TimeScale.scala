package latis.time

import latis.time.TimeScaleType.TimeScaleType
import latis.units.UnitOfMeasure
import latis.util.RegEx

import java.util.Date
import java.util.GregorianCalendar
import java.util.TimeZone

/**
 * Model a time scale as the number of TimeUnit-s from an epoch (start date, i.e. time zero).
 * The TimeScaleType will determine how leap seconds are dealt with (or other calendar differences?).
 */
class TimeScale(val epoch: Date, val unit: TimeUnit, val tsType: TimeScaleType) extends UnitOfMeasure("TODO") {
  //TODO: consider using millis for epoch instead of Date
  
  override def toString() = {
    val sb = new StringBuilder()
    sb.append(unit.name)
    sb.append(" since ")
    sb.append(TimeFormat.DATE.format(epoch.getTime)) //TODO: include time, e.g. Julian Date starts at noon
    //TODO: override for Julian Date?
    
    sb.toString()
  }
}

object TimeScale {
  //Note, using def instead of lazy val to support tests.
  def JAVA = new TimeScale(new Date(0), TimeUnit.MILLISECOND, TimeScaleType.default)
  def DEFAULT = JAVA
  
  /**
   * Define a special case for Julian date: days since noon Jan 1, 4713 BC.
   * Because Java's default calendar jumps from 1 BC to 1 AD, we need to use year -4712.
   * This seems to work for the times we care about.
   */
  lazy val JULIAN_DATE = {
    val cal = new GregorianCalendar(-4712, 0, 1, 12, 0);
    cal.setTimeZone(TimeZone.getTimeZone("GMT"));
    TimeScale(cal.getTime, TimeUnit.DAY, TimeScaleType.default)
  }
  
  def apply(epoch: Date, unit: TimeUnit, tstype: TimeScaleType): TimeScale = {
    new TimeScale(epoch, unit, tstype)
  }
  
  def apply(epoch: String, unit: TimeUnit, tsType: TimeScaleType): TimeScale = {
    new TimeScale(new Date(TimeFormat.fromIsoValue(epoch).parse(epoch)), unit, tsType)
  }
  
  def apply(ts: TimeScale, tsType: TimeScaleType): TimeScale = {
    new TimeScale(ts.epoch, ts.unit, tsType)
  }
  
  /**
   * Make TimeScale from "unit since epoch" or time format String.
   * Assume Native TimeScaleType (no leap second consideration), for now.
   */
  def apply(scale: String): TimeScale = {
    val regex = ("("+RegEx.WORD+")" + """\s+since\s+""" + """(-?[0-9]{4}-[0-9]{2}-[0-9]{2}\S*)""").r
    scale.trim match {
      case regex(unit, epoch) => TimeScale(epoch, TimeUnit.withName(unit), TimeScaleType.default)
      case s: String if (s.startsWith("Julian")) => JULIAN_DATE
      case _ => {
        //assume formatted time (http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html)
        //TODO: test for valid TimeFormat
        TimeScale.DEFAULT
      }
    }
  }
}