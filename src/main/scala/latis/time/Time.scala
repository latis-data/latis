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


class Time(timeScale: TimeScale = TimeScale.DEFAULT, metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractScalar(metadata, data) { 

  //Note: there is a one-to-one mapping between java time (ms since 1970) and formatted time.
  //Leap second considerations do not apply.
  //Whether TimeScale.JAVA considers leap seconds is another matter.
  
  def getUnits = timeScale
  
  def convert(scale: TimeScale): Time = TimeConverter(this.timeScale, scale).convert(this)
  
  def toIso: String = TimeFormat.ISO.format(getJavaTime)
  
  def format(format: String): String = TimeFormat(format).format(getJavaTime)
  
  def format(format: TimeFormat): String = format.format(getJavaTime)
  
  def getJavaTime: Long = getData match {
    case num: NumberData => convert(TimeScale.JAVA).getNumberData.longValue
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
      case Some(s) => compare(Time.fromIso(s)) //Make Time from ISO formatted time string, convert to our time scale
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

/*
 * ways to make time from data (e.g. in adapters, operations)
 * - 
 * - 
 * 
 */

object Time {
  import scala.collection.mutable.HashMap
  //TODO: DEFAULT vs JAVA time scale
  
  def fromIso(s: String): Time = Time(isoToJava(s))
  //TODO: make sure format is valid
  
  def isoToJava(s: String): Long = {
    /*
     * javax.xml.bind.DatatypeConverter supported formats include:
     * yyyy
     * yyyy-MM
     * yyyy-MM-dd
     * yyyy-MM-ddTHH:mm:ss
     * yyyy-MM-ddTHH:mm:ss.S (unlimited decimal places)
     */
//    val cal = javax.xml.bind.DatatypeConverter.parseDateTime(s)
//    cal.setTimeZone(TimeZone.getTimeZone("GMT")) //Assume UTC. //TODO: support other time zones?
//    cal.getTimeInMillis()
    
    TimeFormat.fromIsoValue(s).parse(s)
  }
  

  
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
  
  /*
   * TODO: clean up time const
   * should 'type' be  part of metadata? 
   *   or should type be handled within the adapter?
   * that may also be the only case where we need Time without data - orig ds
   * 
   * other cases where we want to specify type? base on data type only?
   */
  def apply(vtype: String, md: Metadata, data: Data = EmptyData): Time = {
    //this is for tsml orig dataset template, no data
    if (vtype == "text") {
      val md2 = md.get("units") match {
        //Assume length is not set, for now. TODO: obey tsml defined length?
        case Some(u) => {
          //TODO: make sure units are valid TimeFormat
          //Get the length of a String representation using these time units (i.e. format).
          //Don't count the single quotes used around the literals (such as the "T" time marker) 
          //  as required by Java's SimpleDataFormat.
          val length = u.filter(_ != ''').length
          md + ("length" -> length.toString)
        }
        case None => md + ("units" -> TimeFormat.ISO.toString) + ("length" -> "23")
      }
      //Note, formatted times will use the default numerical time units as needed.
      new Time(TimeScale.DEFAULT, md2, data) with Text
      
    } else { //Numeric time
      var md2 = md
      val scale = md.get("units") match {
        case Some(u) => TimeScale(u)
        case None    => {
          md2 = md + ("units" -> TimeScale.DEFAULT.toString)
          TimeScale.DEFAULT
        }
      }
      vtype match {
        case "real"    => new Time(scale, md2, data) with Real
        case "integer" => new Time(scale, md2, data) with Integer
      }
    }
  }
  
  
  //TODO: builder called on template instead of constructing from template's metadata
  //not like scala builder
  // template.buildWithData(data)?
  //  apply(data): template(data)? too spooky? kinda like it
  // Builder.buildFromTemplate(template, data)?
  // 
  
  
  def apply(md: Metadata): Time = {
    var metadata = md
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => {
        //Use default time scale, add units to metadata
        metadata = md + ("units" -> TimeScale.DEFAULT.toString)
        TimeScale.DEFAULT
      }
    }
    new Time(scale, metadata)
  }
  
  def apply(md: Metadata, value: AnyVal): Time = {
    var metadata = md
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => {
        //Use default time scale, add units to metadata
        metadata = md + ("units" -> TimeScale.DEFAULT.toString)
        TimeScale.DEFAULT
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
  
      
  def apply(scale: TimeScale, value: AnyVal): Time = {
    //make some metadata
    val md = Metadata(Map("name" -> "time", "units" -> scale.toString))
    value match {
      case _: Float => new Time(scale, md, Data(value)) with Real
      case _: Double => new Time(scale, md, Data(value)) with Real
      case _: Int => new Time(scale, md, Data(value)) with Integer
      case _: Long => new Time(scale, md, Data(value)) with Integer
    }
  }

  def apply(value: AnyVal): Time = Time(TimeScale.DEFAULT, value)
  
  def apply(date: Date): Time = Time(date.getTime())
  
  /*
   * TODO: Time as Tuple. 
   * But Time extends Scalar.
   * Always convert to scalar?
   * How to use as type without data, then apply data? 
   *   have been avoiding letting tuple contain data
   * use case: timed see netcdf, DATE (yyyyDDD) and TIME (seconds)
   *   data end up in column oriented cache
   *   can't make a scalar type for the template that can pull in multiple values
   * 
   * can tuple still play as a scalar?
   *   compare to magnetic field magnitude
   *   compare to value and uncertainty tuple
   *   compare to bin average with min, max...
   *   always use 1st element of tuple in scalar context?
   * Derived field?
   *   operation
   * 
   * Combine all text components delimited with comma.
   * Add numeric component, converted to ms
   */

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
  
    
  //TODO: move to util?
//  def stringsToNumbers(ss: Seq[String]): Seq[AnyVal] = ss.map(stringToNumber(_))
//  def stringToNumber(s: String): AnyVal = {
//    try {s.toLong}  //try converting to Long
//    catch {
//      case e: Exception => {
//        try {s.toDouble}  //try converting to Double
//        catch {
//          case e: Exception => throw new RuntimeException("Can't convert String into number: " + s)
//        }
//      }
//    }
//  }
  
  
//TODO: 
//  val NOW = new Time() {
//    override def getTime() = (new Date()).getTime().toDouble
//  }
}