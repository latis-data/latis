package latis.time

import java.util.Date
import latis.time.TimeScaleType._
import java.text.SimpleDateFormat
import scala.util.matching.Regex
import latis.units.UnitOfMeasure
import latis.util.RegEx

class TimeScale(val epoch: Date, val unit: TimeUnit, val tsType: TimeScaleType) extends UnitOfMeasure("TODO") {
  
  override def toString() = {
    val sb = new StringBuilder()
    sb.append(unit.name)
    sb.append(" since ")
    sb.append(TimeFormat.DATE.format(epoch))
    
    sb.toString()
  }
}

object TimeScale {
  //TODO: case objects?
  val JAVA = new TimeScale(new Date(0), TimeUnit.MILLISECOND, TimeScaleType.NATIVE)
  val DEFAULT = JAVA
  
  def apply(epoch: Date, unit: TimeUnit, tstype: TimeScaleType): TimeScale = {
    new TimeScale(epoch, unit, tstype)
  }
  
  def apply(epoch: String, unit: TimeUnit, tsType: TimeScaleType): TimeScale = {
    new TimeScale(TimeFormat.DATE.parse(epoch), unit, tsType)
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
    //TODO: apparently scala regex will extract group for nested ()s
    val regex = ("("+RegEx.WORD+")" + """\s+since\s+""" + """([0-9]{4}-[0-9]{2}-[0-9]{2}\S*)""").r
    scale.trim match {
      case regex(unit, epoch) => TimeScale(epoch, TimeUnit.withName(unit), TimeScaleType.NATIVE)
      case _ => {
        //assume formatted time (http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html)
        TimeScale.JAVA
      }
    }
  }
}