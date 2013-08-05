package latis.time

import latis.dm.Real
import java.util.Date
import latis.metadata.Metadata
import latis.data.value.DoubleValue
import latis.data.EmptyData
import latis.data.Data
import latis.metadata.EmptyMetadata
import latis.util.RegEx

class Time extends Real {
  
  protected var timeScale: TimeScale = TimeScale.DEFAULT
  
  def getUnits() = timeScale
  
  def convert(scale: TimeScale): Time = {
    TimeConverter(this.timeScale, scale).convert(this)
  }
  
  def toIso: String = {
    val time = convert(TimeScale.JAVA).doubleValue.toLong //TODO: get long from Data
    val date = new java.util.Date(time)
    TimeFormat.DATETIME.format(date)
  }
  
  def format: String = {
    TimeFormat(metadata("format")).format(doubleValue)
  }
  
  //override to deal with formatted time strings  
  override def compare(that: String): Int = {
//  that match {
//    case RegEx.TIME(time) => { NOT matching, "findFirstIn" seems to work
//      //convert ISO to java time then to our time scale
//      //TODO: convert ISO string to java time, then convert to current time scale
//      val t = Time(javax.xml.bind.DatatypeConverter.parseDateTime(that).getTimeInMillis().toDouble)
//      //TODO: make sure parser does UTC
//      //TODO: reuse converter
//      val other = t.convert(timeScale).toDouble
//      compare(other)
//    }
    RegEx.TIME findFirstIn that match {
      case Some(s) => {
        val t = Time(javax.xml.bind.DatatypeConverter.parseDateTime(s).getTimeInMillis().toDouble)
        val other = t.convert(timeScale).doubleValue
        compare(other)
      }
      case _ => compare(that.toDouble) //TODO: potential parse error
    }
  }
}

object Time {
  import scala.collection.mutable.HashMap
  //TODO: DEFAULT vs JAVA time scale
  
  //val defaultMd = HashMap(("name", "time"), ("type", "Time"))
  //don't change name here
  
  def apply(md: Metadata): Time = {
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => TimeScale.JAVA
    }
    val t = new Time
    t._data = EmptyData
    t._metadata = md
    t.timeScale = scale
    t
  }
    
  def apply(md: Metadata, values: Seq[Double]): Time = {
    val t = new Time
    t._metadata = md
    t._data = Data(values)
    t.timeScale = TimeScale.JAVA
    t
  }
    
  def apply(md: Metadata, value: Double): Time = {
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => TimeScale.JAVA
    }
    val t = new Time
    t._data = DoubleValue(value)
    t._metadata = md
    t.timeScale = scale
    t
  }
  
  def apply(value: Double, units: String): Time = {
    val t = new Time
    t._data = DoubleValue(value)
    t._metadata = Metadata(Map(("units", units))) //Metadata(defaultMd + (("units", units)))
    t.timeScale = TimeScale(units)
    t
  }
  
  def apply(value: Double, scale: TimeScale): Time = {
    val t = new Time
    t._data = DoubleValue(value)
    t._metadata = Metadata(Map(("units", scale.toString)))
    t.timeScale = scale
    t
  }
  
  /**
   * Parse time given units as SimpleDateFormat using Java (default) time scale.
   */
  def apply(value: String, units: String): Time = {
    if (units.contains(" since ")) Time(value.toDouble, units)
    else {
      val format = new TimeFormat(units)
      Time(format.parse(value).getTime(), TimeScale.JAVA)
    }
  }
  
  def apply(md: Metadata, value: Double, scale: TimeScale): Time = {
    val t = new Time
    t._data = DoubleValue(value)
    t._metadata = md
    t.timeScale = scale
    t
  }
  
  /**
   * Parse time given units as SimpleDateFormat using Java (default) time scale.
   */
  def apply(md: Metadata, value: String): Time = {
    /*
     * TODO: units vs format
     * units is prime metadata (within "metadata" element)
     * format could just be an instruction to the adapter (xml attribute?)
     */
    md.get("units") match {
      case Some(u) => {
        if (u.contains(" since ")) Time(md, value.toDouble, TimeScale.JAVA)
        else {
          val format = new TimeFormat(u)
          Time(md, format.parse(value).getTime(), TimeScale.JAVA)
          //TODO: consider units and format attributes
        }
      }
      case None => Time(md, value.toDouble, TimeScale.JAVA) //assume default units
        //throw new RuntimeException("Time Metadata is missing units.")
    }
  }
  
  /*
   * TODO: use "format" if formatted, default to java units, override by also setting "units"?
   */
  
  /**
   * Milliseconds since 1970, native Java time.
   */
  def apply(value: Double): Time = Time(value, TimeScale.JAVA)
  
  def apply(date: Date): Time = Time(date.getTime().toDouble)
  
//TODO:
//  val NOW = new Time() {
//    override def getTime() = (new Date()).getTime().toDouble
//  }
}