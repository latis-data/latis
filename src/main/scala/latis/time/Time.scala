package latis.time

import latis.dm._
import java.util.Date
import latis.metadata._
import latis.data.value._
import latis.data._
import latis.util.RegEx
import java.util.TimeZone
import latis.metadata.VariableMetadata
import latis.data.value.LongValue
import scala.collection.immutable.StringOps


class Time(timeScale: TimeScale = TimeScale.JAVA, metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractScalar(metadata, data) { 

  //Note: there is a one-to-one mapping between java time (ms since 1970) and formatted time.
  //Leap second considerations do not apply when going between the numeric and formatted form.
  //Whether TimeScale.JAVA considers leap seconds is based on the time.scale.type property.
  
  def getUnits = timeScale
  
  def convert(scale: TimeScale): Time = TimeConverter(this.timeScale, scale).convert(this)
  
  def toIso: String = TimeFormat.ISO.format(getJavaTime)
  
  def format(format: String): String = TimeFormat(format).format(getJavaTime)
  
  def format(format: TimeFormat): String = format.format(getJavaTime)
  
  def getJavaTime: Long = getData match {
    //Note, converts from the data's time scale (with it's own type) to JAVA time which uses the time.scale.type property.
    case num: NumberData => convert(TimeScale.JAVA).getNumberData.longValue
    
    //Note, time scale type doesn't matter. If either is "NATIVE", leap seconds will not be applied.
    //  If both are UTC, they are not different. TAI is not supported for Text Times.
    case text: TextData => {
      val format = getMetadata.get("units") match {  //note: using units for format 
        case Some(f) => f
        case None => TimeFormat.ISO.toString //default to ISO format
      }
      TimeFormat(format).parse(text.stringValue)
    }
  }

  override def compare(that: Scalar): Int = that match {
    case t: Time => {
      //Convert 'that' Time to our time scale.
      //Note, converted Times will have numeric (double or long) data values.
      val otherData = t.convert(timeScale).getNumberData
      //Base comparison on our type so we can get the benefit of double precision or long accuracy
      getData match {
        case LongValue(l) => l compare otherData.longValue
        case NumberData(d) => d compare otherData.doubleValue
        case _: TextData => getJavaTime compare otherData.longValue
      }
    }
    case _ => throw new Error("Can't compare " + this + " with " + that)
  }
  
  //override to deal with ISO formatted time strings  
  override def compare(that: String): Int = {
    //TODO: look for units and see if 'that' matches...
    RegEx.TIME.r findFirstIn that match {
      //If the string matches the ISO format
      //Make Time from ISO formatted time string (based on time.scale.type), convert to our time scale
      case Some(s) => compare(Time.fromIso(s)) 
      //Otherwise assume we have a numeric value in our time scale
      case _ => getData match {
        case LongValue(l) => l compare that.toLong
        //TODO: allow 'that' to be double even if this is Integer?, careful about precision loss
        //case LongValue(l) => l.toDouble compare that.toDouble
        case NumberData(d) => d compare that.toDouble
        case _: TextData => getJavaTime compare that.toLong
        //TODO: handle format errors
      }
    }
  }

}

//=============================================================================

object Time {
  
  /**
   * Create a Time instance from the given ISO8601 formatted string.
   * Use the time.scale.type property to determine if this should be
   * interpreted as a UTC time or the default NATIVE time.
   */
  def fromIso(s: String): Time = Time(isoToJava(s))
  //TODO: make sure format is valid, use Try?
  
  /**
   * Given an ISO8601 formatted time string, return the number of milliseconds since 1970-01-01.
   * This is independent of time scale type (e.g. leap seconds) since, by our definition,
   * formated times are backed by (and isomorphic with) the java time scale.
   */
  def isoToJava(s: String): Long = TimeFormat.fromIsoValue(s).parse(s)
  

  /**
   * Only used by TsmlAdapter (and tests) as a Variable template in orig Dataset (no Data).
   * Since the Time applies to a Dataset, look for the time_scale_type definition in the Metadata.
   * Default to a NATIVE TimeScaleType as opposed to using the app's default time.scale.type.
   */
  def apply(vtype: String, md: Metadata, data: Data = EmptyData): Time = {
    //data defaults to native time scale type
    val tsType = TimeScaleType.withName(md.getOrElse("time_scale_type", "NATIVE")) 
    
    if (vtype == "text") {
      var format = ""
      val md2 = md.get("units") match {
        //Assume length is not set, for now. TODO: obey tsml defined length?
        case Some(u) => {
          //TODO: make sure units are valid TimeFormat
          //Get the length of a String representation using these time units (i.e. format).
          //Don't count the single quotes used around the literals (such as the "T" time marker) 
          //  as required by Java's SimpleDataFormat.
          format = u
          val length = u.filter(_ != ''').length
          md + ("length" -> length.toString)
        }
        case None => {
          format = TimeFormat.ISO.toString
          md + ("units" -> format) + ("length" -> format.filter(_ != ''').length.toString)
        }
      }
      //Note, formatted times will use the default numerical time units as needed.
      val ts = TimeScale(format) //built from format so we can preserve it in toString
      new Time(ts, md2, data) with Text
      
    } else { //Numeric time
      var md2 = md
      val scale = md.get("units") match {
        case Some(u) => TimeScale(u)
        case None    => {
          //default to java time with native time scale type
          //TODO: not likely useful in the wild, but simplifies tests. 
          val units = "milliseconds since 1970-01-01" 
          md2 = md + ("units" -> units)
          TimeScale(units)
        }
      }
      vtype match {
        case "real"    => new Time(scale, md2, data) with Real
        case "integer" => new Time(scale, md2, data) with Integer
      }
    }
  }

  
  def apply(md: Metadata): Time = {
    var metadata = md
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => {
        //Use default time scale, add units to metadata
        metadata = md + ("units" -> TimeScale.JAVA.toString)
        TimeScale.JAVA
      }
    }
    new Time(scale, metadata)
  }
  
  /**
   * Generally used when making a new Time from an old one. Copy metadata, add new value.
   * TODO: use or remove 'type' in metadata (e.g. real, integer, text)
   * TODO: interpret value in context of units
   */
  def apply(md: Metadata, value: Any): Time = {
    var metadata = md
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => {
        //Use default time scale, add units to metadata
        metadata = md + ("units" -> TimeScale.JAVA.toString)
        TimeScale.JAVA
      }
    }
    value match {
      case _: Float => new Time(scale, metadata, Data(value)) with Real
      case _: Double => new Time(scale, metadata, Data(value)) with Real
      case _: Int => new Time(scale, metadata, Data(value)) with Integer
      case _: Long => new Time(scale, metadata, Data(value)) with Integer
      case _: StringOps => new Time(scale, metadata, Data(value.toString)) with Text
    }
  }
  
      
  def apply(scale: TimeScale, value: Any): Time = {
    //make some metadata
    val md = Metadata(Map("name" -> "time", "units" -> scale.toString))
    value match {
      case _: Float => new Time(scale, md, Data(value)) with Real
      case _: Double => new Time(scale, md, Data(value)) with Real
      case _: Int => new Time(scale, md, Data(value)) with Integer
      case _: Long => new Time(scale, md, Data(value)) with Integer
    }
  }

  def apply(value: String): Time = Time(TimeScale.JAVA, value)
  def apply(value: AnyVal): Time = Time(TimeScale.JAVA, value)
  
  def apply(date: Date): Time = Time(date.getTime())

//============= old stuff =================

  
  //may have no data, used as a template in adapters
//  def apply(md: Metadata, data: Data = EmptyData): Time = {
//  //def apply(md: Metadata): Time = {
//    var metadata = md
//    val scale = md.get("units") match {
//      case Some(u) => TimeScale(u)
//      case None => {
//        //Use default time scale, add units to metadata
//        //TODO: if type is text, set units to ISO
//        
//        metadata = new VariableMetadata(md.getProperties + ("units" -> TimeScale.DEFAULT.toString))
//        TimeScale.DEFAULT
//      }
//    }
//    //Mixin the appropriate type
//    md.get("type") match {
//      case Some(s) => s.toLowerCase match {
//        case "real" => new Time(scale, metadata, EmptyData) with Real
//        case "integer" => new Time(scale, metadata, EmptyData) with Integer
//        case "text" => {
//          //TODO: Use the units length to define the text length, but units from above not ISO
//          //TODO: what if the metadata already defines the text length?
//          //val length = metadata("units").length
//          //val md2 = new VariableMetadata(md.getProperties + ("length" -> length.toString))
//          new Time(scale, metadata, EmptyData) with Text
//        }
//        case _ => throw new RuntimeException("Unsupported Time type: " + s)
//      }
//      //default to Real
//      case None => new Time(scale, md, EmptyData) with Real
//    }
//  }
  
  
//  def apply(md: Metadata, value: String): Time = {
//    md.get("units") match {
//      case Some(u) => {
//        //If it is a numeric time, units should have "since".
//        //TODO: special case for JulianDate? 
//        if (u.contains(" since ")) {
//          //look for md("type"), default to Real
//          md("type") match {
//            case "integer" => new Time(TimeScale(u), md, Data(value.toLong)) with Integer
//            case _ => new Time(TimeScale(u), md, Data(value.toDouble)) with Real
//          }
//        }
//        //Otherwise, store data as StringValue.
//        //Use java time scale so we can count on using SimpleDateFormat conversions.
//        //TODO: consider implications of ignoring leap seconds for formatted times, 
//        //  could java format conversions still work for UTC scale?
//        else new Time(TimeScale.JAVA, md, StringValue(value)) with Text
//      }
//      //No units specified, assume default numeric units or ISO format
//      case None => md("type") match {
//        case "integer" => new Time(TimeScale.DEFAULT, md, Data(value.toLong)) with Integer
//        case "real"    => new Time(TimeScale.DEFAULT, md, Data(value.toDouble)) with Real
//        case "text"    => {
//          new Time(TimeScale.DEFAULT, md, Data(value)) with Text
//        }
//        //TODO: add length metadata based on length of units format
//        //  default length of 23 (ISO format), but actual unit string for ISO has quotes around T, so 25
//        //  consider time zone? 'Z'
//      }
//    }
//  }
  
}