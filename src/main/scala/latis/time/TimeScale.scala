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

  private var format = ""

  override def toString() = {
    if (format.nonEmpty) format
    else {
      val sb = new StringBuilder()
      tsType.toString match {
        case "UTC" => sb.append("UTC ")
        case "TAI" => sb.append("TAI ")
        case _ => //don't label native units
      }
      sb.append(unit.name)
      sb.append(" since ")
      sb.append(TimeFormat.DATE.format(epoch.getTime)) //TODO: include time, e.g. Julian Date starts at noon
      //TODO: override for Julian Date?

      sb.toString()
    }
  }
}

object TimeScale {
  //Note, using def instead of lazy val to support tests with varying default TimeScaleType.
  def JAVA = new TimeScale(new Date(0), TimeUnit.MILLISECOND, TimeScaleType.default)

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

  /**
   * Make new TimeScale from an existing TimeScale but with a specified TimeScaleType.
   */
  def apply(ts: TimeScale, tsType: TimeScaleType): TimeScale = {
    new TimeScale(ts.epoch, ts.unit, tsType)
  }

  /**
   * Make TimeScale from "(tsType) unit since epoch", special name, or time format String adhering to java.util.SimleDateFormat.
   * "tsType" is the optional (case-insensitive) TimeScaleType: "UTC", "TAI" or "NATIVE".
   * If tsType is not specified for a numeric unit (unit since epoch), NATIVE will be used.  Note, the time.scale.type property will not be used
   * because units are considered to be a property of the dataset. The property is designed to specify behavior in LaTiS.
   * e.g. how to interpret a time selection. If a "naively" written dataset were interpreted as UTC, unexpected leap second offsets will be introduced.
   * Special names supported include:
   *   Julian Date
   * Formatted time "units" with use the time.scale.type property for now since there is currently no mechanism to specify the type in the format string.
   * This will likely need to change. (LATIS-322)
   */
  def apply(scale: String): TimeScale = {
    scale.split(" since ") match {
      case Array(s, epoch) => s.split("""\s""") match {
        case Array(tstype, unit) => TimeScale(epoch, TimeUnit.withName(unit.toLowerCase), TimeScaleType.withName(tstype.toUpperCase))
        case Array(unit) => TimeScale(epoch, TimeUnit.withName(unit.toLowerCase), TimeScaleType.NATIVE)
        case _ => throw new UnsupportedOperationException("Time unit has more than 2 components before the 'since': " + scale)
      }
      case Array(s) => s match {
        case s: String if (s.toLowerCase.startsWith("julian")) => JULIAN_DATE //TODO: can we interpret JD as UTC?
        case _ => {
          //formatted
          val ts = TimeScale.JAVA //will get tsType from time.scale.type property
          //hack format into TimeScale so we can use it when printing units
          ts.format = s
          ts
        }
      }
      case _ => throw new UnsupportedOperationException("Time unit contains 'since' more than once: " + scale)
    }
  }
}