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

//class RealTime(timeScale: TimeScale = TimeScale.DEFAULT, metadata: Metadata = EmptyMetadata, data: Data = EmptyData)
//  extends Time(timeScale, metadata, data) with Real 

class Time(timeScale: TimeScale = TimeScale.DEFAULT, metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractScalar(metadata, data) { 
  //TODO: generalize scale to units for all numeric data
  
  //TODO: add utc and tai tsml types?
  
  /*
   * TODO: support double, long, or string representation
   * help with format vs units problem?
   * Does time have type Real or real have type Time?
   * consider multiple time variables
   *   only THE indep time should use the tsml time element?
   *   others should be declared as text, real,... 
   *   type="Time" is like type="my.custom.Variable"
   *   but presumably the custom var would extend Real...
   * 
   *   
   * Any other use cases where we want to represent data in various forms?
   * or is Time the oddball?
   *   stellar temperature and spectral type?
   *   what else do we have symbolic vs numeric representations?
   * But still something more profound about time as a concept
   *   why is it diff than temperature?
   *   is the desire for integer support only a precision concern that could apply to any var? seems so
   *     but they could be declared as an integer
   *     but not a specialized subclass as Time is
   *   and the desire for text support is because of symbolic string representation
   *   
   * Back to thinking of time as a real
   *   what about other time variables (not the time domain)?
   *   can we treat them as text?
   *   but what if we want to query on them?
   *   do we need Text/Symbolic/FormattedTime and RealTime?
   *   like custom vars, we should be able to use type="Time" for class to construct
   *   goal should be to represent original form, avoid transforming (e.g. string to java time)
   * 
   */
  
  //protected var timeScale: TimeScale = TimeScale.DEFAULT
  
  def getUnits = timeScale
  
  def convert(scale: TimeScale): Time = TimeConverter(this.timeScale, scale).convert(this)
  
  def toIso: String = TimeFormat.ISO.format(getJavaDate)
  
  //def format: String = TimeFormat(getMetadata("format")).format(getJavaDate)
  
  def format(format: String): String = TimeFormat(format).format(getJavaDate)
  
  def getJavaDate: java.util.Date = new java.util.Date(getJavaTime)
  
  def getJavaTime: Long = getData match {
    case num: NumberData => convert(TimeScale.JAVA).getNumberData.longValue
    case text: TextData => {
      val format = getMetadata.get("units") match {  //note: using units for format //TODO: consider use of "format"
        case Some(f) => f
        case None => TimeFormat.ISO.toString //default to ISO format
      }
      TimeFormat(format).parse(text.stringValue).getTime
    }
  }
  
  
  def compare(that: Time): Int = {
    //Convert 'that' Time to our time scale.
    //Note, converted Times will have numeric (double or long) data values.
    val otherData = that.convert(timeScale).getNumberData
    //Base comparison on our type so we can get the benefit of double precision or long accuracy
    getData match {
      case LongValue(l) => l compare otherData.longValue
      case NumberData(d) => d compare otherData.doubleValue
      case _: TextData => getJavaTime compare otherData.longValue
    }
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
        case NumberData(d) => d compare that.toDouble
        case _: TextData => getJavaTime compare that.toLong
        //TODO: handle format errors
      }
    }
  }

}

//=============================================================================



//=============================================================================

object Time {
  import scala.collection.mutable.HashMap
  //TODO: DEFAULT vs JAVA time scale
  
  def fromIso(s: String): Time = Time(isoToJava(s))
  //TODO: make sure format is valid
  
  def isoToJava(s: String): Long = {
    val cal = javax.xml.bind.DatatypeConverter.parseDateTime(s)
    cal.setTimeZone(TimeZone.getTimeZone("GMT")) //Assume UTC. //TODO: support other time zones?
    cal.getTimeInMillis()
  }
  
  
  //def apply(scale: TimeScale, md: Metadata, data: Data) = new Time(scale, md, data)
  //need to know what scalar type to mixin
  
  //no data, used as a template in adapters
  def apply(md: Metadata, data: Data = EmptyData): Time = {
    var metadata = md
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => {
        //Use default time scale, add units to metadata
        metadata = new VariableMetadata(md.getProperties + ("units" -> TimeScale.DEFAULT.toString))
        TimeScale.DEFAULT
      }
    }
    //Mixin the appropriate type
    md.get("type") match {
      case Some(s) => s.toLowerCase match {
        case "real" => new Time(scale, md, data) with Real
        case "integer" => new Time(scale, md, data) with Integer
        case "text" => new Time(scale, md, data) with Text
        case _ => throw new RuntimeException("Unsupported Time type: " + s)
      }
      //default to Real
      case None => new Time(scale, md, data) with Real
    }
  }
  
  //def apply(md: Metadata, typ: String): Time 
  
  def apply(md: Metadata, value: AnyVal): Time = {
    var metadata = md
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => {
        //Use default time scale, add units to metadata
        metadata = new VariableMetadata(md.getProperties + ("units" -> TimeScale.DEFAULT.toString))
        TimeScale.DEFAULT
      }
    }   
    value match {
      case _: Double => new Time(scale, metadata, Data(value)) with Real
      case _: Long => new Time(scale, metadata, Data(value)) with Integer
    }
  }
  
  def apply(md: Metadata, values: Seq[AnyVal]): Time = {
    var metadata = md
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => {
        //Use default time scale, add units to metadata
        metadata = new VariableMetadata(md.getProperties + ("units" -> TimeScale.DEFAULT.toString))
        TimeScale.DEFAULT
      }
    }
    values.head match {
      case _: Double => new Time(scale, metadata, Data(values)) with Real
      case _: Long => new Time(scale, metadata, Data(values)) with Integer
    }
  }
  
      
  def apply(scale: TimeScale, value: AnyVal): Time = {
    //make some metadata
    val md = Metadata(Map("name" -> "time", "units" -> scale.toString))
    value match {
      case _: Double => new Time(scale, md, Data(value)) with Real
      case _: Long => new Time(scale, md, Data(value)) with Integer
    }
  }

  def apply(value: AnyVal): Time = Time(TimeScale.DEFAULT, value)
  
  def apply(date: Date): Time = Time(date.getTime())
  
  
//    
//  def apply(md: Metadata, values: Seq[Double], scale: TimeScale): Time = {
//    val t = new Time
//    t._metadata = md
//    t._data = Data(values)
//    t.timeScale = scale
//    t
//  }
  
//  def apply(value: Double, units: String): Time = {
//    val t = new Time
//    t._data = DoubleValue(value)
//    t._metadata = Metadata(Map(("units", units))) //Metadata(defaultMd + (("units", units)))
//    t.timeScale = TimeScale(units)
//    t
//  }
  
//  /**
//   * Parse time given units as SimpleDateFormat using Java (default) time scale.
//   */
//  def apply(value: String, units: String): Time = {
//    if (units.contains(" since ")) Time(value.toDouble, units)
//    else {
//      val format = new TimeFormat(units)
//      Time(format.parse(value).getTime(), TimeScale.DEFAULT)
//    }
//  }
//  
//  def apply(md: Metadata, value: Double, scale: TimeScale): Time = {
//    val t = new Time
//    t._data = DoubleValue(value)
//    t._metadata = md
//    t.timeScale = scale
//    t
//  }
  

  def apply(md: Metadata, value: String): Time = {

    md.get("units") match {
      case Some(u) => {
        //If it is a numeric time, units should have "since".
        //TODO: special case for JulianDate? CarringtonRotation...?
        if (u.contains(" since ")) {
          //look for md("type"), default to Real
          md("type") match {
            case "integer" => new Time(TimeScale(u), md, Data(value.toLong)) with Integer
            case _ => new Time(TimeScale(u), md, Data(value.toDouble)) with Real
          }
        }
        //Otherwise, store data as StringValue.
        //Use java time scale so we can count on using SimpleDateFormat conversions.
        //TODO: consider implications of ignoring leap seconds for formatted times, 
        //  could java format conversions still work for UTC scale?
        else new Time(TimeScale.JAVA, md, StringValue(value)) with Text
      }
      //No units specified, assume default numeric units or ISO format
      case None => md("type") match {
        case "integer" => new Time(TimeScale.DEFAULT, md, Data(value.toLong)) with Integer
        case "real"    => new Time(TimeScale.DEFAULT, md, Data(value.toDouble)) with Real
        case "text"    => new Time(TimeScale.DEFAULT, md, Data(value)) with Text
//        {
//          //add default length of 23 (ISO format)
//          //TODO: need to set in adapter so we can establish record size
//          val md2 = md.get("length") match {
//            case Some(_) => md
//            case None => Metadata(md.getProperties ++ Map("length" -> "23"))
//          }
//          new Time(TimeScale.DEFAULT, md2, Data(value)) with Text
//        }
      }
    }
  }
  
  def fromStrings(md: Metadata, values: Seq[String]): Time = {
    md.get("units") match {
      case Some(u) => {
        if (u.contains(" since ")) Time(md, stringsToNumbers(values))  //numeric units
        else { //formatted time
          //store as strings, Time with Text
          //assume type is text
          new Time(TimeScale.DEFAULT, md, Data(values)) with Text
          
//          //convert to java time scale for now, //TODO: use arbitrary default time scale
//          val scale = TimeScale.JAVA
//          //make sure units metedata is correct //TODO: add type="integer" to metadata?
//          val md2 = Metadata(md.getProperties + ("units" -> scale.toString))
// //TODO: since we are changing the units, at least let us write with native format
// //  use 'format'? also could be used by format_time filter         
//          //parse times into longs
//          val format = TimeFormat(u)
//          val times: Seq[Long] = values.map(format.parse(_).getTime())
//          //note, tempted to delegate to Time(Metadata, Seq[Any]) to get default time scale,
//          //  but we are assuming JAVA time here
//          new Time(scale, md2, Data(times)) with Integer
        }
      }
      case None => Time(md, values.map(_.toDouble))
    }
  }
    
  //TODO: use in Data object?
  //def stringToNumberData(s: String): Data = Data(stringToNumber(s))
  //def stringsToNumberData(ss: Seq[String]): Data = Data(ss.map(stringToNumber(_)))
  def stringsToNumbers(ss: Seq[String]): Seq[AnyVal] = ss.map(stringToNumber(_))
  def stringToNumber(s: String): AnyVal = {
    try {s.toLong}  //try converting to Long
    catch {
      case e: Exception => {
        try {s.toDouble}  //try converting to Double
        catch {
          case e: Exception => throw new RuntimeException("Can't convert String into number: " + s)
        }
      }
    }
  }
  
  
  
//TODO:
//  val NOW = new Time() {
//    override def getTime() = (new Date()).getTime().toDouble
//  }
}