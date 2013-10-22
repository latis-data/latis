package latis.time

import latis.dm._
import java.util.Date
import latis.metadata._
import latis.data.value._
import latis.data._
import latis.util.RegEx
import java.util.TimeZone

class Time(timeScale: TimeScale = TimeScale.DEFAULT, metadata: Metadata = EmptyMetadata, data: Data = EmptyData) extends 
  Variable2(metadata, data) {
  
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
   * Can we use traits for all vars?
   *   new Time with Real or new Real with Time?
   *   could we keep the data model out of Time for the purpose of sharing?
   *   Real as trait with self type of Variable
   *   there's no real impl in scalars
   * Could we use traits for Tuple and Function?
   *   probably not, have state: vars, domain/range
   *   could do via methods...
   * 
   * factory constructors
   *   new Variable(md, data) with Real
   *   no need for setting _metadata, _data?
   *   maybe just for Scalars? new Scalar with...?
   *   but seems like a mismatch
   *   maybe not if its traits for scalars
   *   traits for Functions?
   *     TimeSeries? or subclass?
   *     
   * Vector extends Tuple? or new Tuple with Vector trait?
   *   liskov substitution principle?
   *   Vector is the specialized case
   *   Vector is always a Tuple but not any Tuple is a Vector
   *   what do they say about traits/mixins?
   *   Foo with Bar
   *     makes a Foo usable as a Bar
   *     Foo is-a Bar
   *     certainly not the other way around
   *   Variable with Real
   *     definitely seems backwards
   *     Real is a special kind of Variable
   *     this is more like "as-a": threat this Variable as a Real
   *   
   * Time extends Variable? or Scalar?
   *   new Time with Real?
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
   * Consider the Logging trait
   *   I suppose that means the class behaves as a logger
   *   
   * Variable with Real mean it behaves as a Real but the model says that Real is-a Variable
   *   Or is Scalar all the model knows and the trait makes it behave like a certain type
   *     
   *   
   * http://stackoverflow.com/questions/3422606/mixins-vs-composition-in-scala
   *   
   */
  
  //protected var timeScale: TimeScale = TimeScale.DEFAULT
  
  def getUnits = timeScale
  
  def convert(scale: TimeScale): Time = TimeConverter(this.timeScale, scale).convert(this)
  
  def toIso: String = TimeFormat.ISO.format(getJavaDate)
  
  //def format: String = TimeFormat(getMetadata("format")).format(getJavaDate)
  
  
  def getJavaDate: java.util.Date = new java.util.Date(getJavaTime)
  
  def getJavaTime: Long = getData match {
    case num: NumberData => convert(TimeScale.JAVA).getData.asInstanceOf[NumberData].longValue
    case text: TextData => TimeFormat(getMetadata("units")).parse(text.stringValue).getTime
  }
  
  
  def compare(that: Time): Int = {
    //Convert 'that' Time to our time scale.
//Note, converted Times will have numeric (double or long) data values.
    val otherData = that.convert(timeScale).getData.asInstanceOf[NumberData]
    //Base comparison on our type so we can get the benefit of double precision or long accuracy
    getData match {
      case LongValue(l) => l compare otherData.longValue
      case NumberData(d) => d compare otherData.doubleValue
      case _: TextData => getJavaTime compare otherData.longValue
    }
  }
  
  //override to deal with ISO formatted time strings  
  override def compare(that: String): Int = {
    RegEx.TIME findFirstIn that match {
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
  
  //val defaultMd = HashMap(("name", "time"), ("type", "Time"))
  //don't change name here
  
  def fromIso(s: String): Time = Time(isoToJava(s))
  //TODO: make sure format is valid
  
  def isoToJava(s: String): Long = {
    val cal = javax.xml.bind.DatatypeConverter.parseDateTime(s)
    cal.setTimeZone(TimeZone.getTimeZone("GMT")) //Assume UTC. //TODO: support other time zones?
    cal.getTimeInMillis()
  }
  
  
  def apply(scale: TimeScale, md: Metadata, data: Data) = new Time(scale, md, data)
  
  def apply(md: Metadata): Time = {
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => TimeScale.DEFAULT  //TODO: add units to metadata
    }
    new Time(scale, md)
  }
    
//  def apply(md: Metadata, values: Seq[Double]): Time = {
//    val scale = md.get("units") match {
//      case Some(u) => TimeScale(u)
//      case None => TimeScale.DEFAULT
//    }
//    val t = new Time
//    t._metadata = md
//    t._data = Data(values)
//    t.timeScale = scale
//    t
//  }
//    
//  def apply(md: Metadata, values: Seq[Double], scale: TimeScale): Time = {
//    val t = new Time
//    t._metadata = md
//    t._data = Data(values)
//    t.timeScale = scale
//    t
//  }
    
  def apply(md: Metadata, value: Double): Time = {
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => TimeScale.DEFAULT //TODO: add units to metadata
    }
    new Time(scale, md, DoubleValue(value))
  }
  
//  def apply(value: Double, units: String): Time = {
//    val t = new Time
//    t._data = DoubleValue(value)
//    t._metadata = Metadata(Map(("units", units))) //Metadata(defaultMd + (("units", units)))
//    t.timeScale = TimeScale(units)
//    t
//  }
  
  def apply(scale: TimeScale, value: Double): Time = {
    val md = Metadata(Map(("units", scale.toString)))
    val data = DoubleValue(value)
    new Time(scale, md, data)
  }
  
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
  
  /**
   * Parse time given units as SimpleDateFormat using Java (default) time scale.
   */
  def apply(md: Metadata, value: String): Time = {
    /*
     * TODO: units vs format
     * units is prime metadata (within "metadata" element)
     * format could just be an instruction to the adapter (xml attribute?)
     * Does it make sense to have "format" in the metadata?
     *   maybe for ascii output
     *   or is it just an instruction to the writer
     *   consider "Name (units)" header, format makes sense for units here
     *     special case: if hasFormat...
     *   mechanism for user request to inform format
     *     TSS1 problem, format filter sets units, writer doesn't know if filter set it or if to use writer default
     *   formatted time akin to diff grid project - refers to same thing, just diff representation?
     *     converting string to unix time like projecting onto standard grid
     *     might lose something in the conversion
     * So does format_time become a true filter like re-gridding?
     *   
     * should format in md suggest that binary have string representation?
     *   consider idl api where user wants yyyyDDD, otherwise need converter from unix time
     * do we store string in Time Data?
     *   Time extends Real
     *   but could always behave by supplying double
     */
    md.get("units") match {
      case Some(u) => {
        //If it is a numeric time, units should have "since".
        //TODO: special case for JulianDate? CarringtonRotation...?
        if (u.contains(" since ")) new Time(TimeScale(u), md, DoubleValue(value.toDouble)) //TODO: allow specification of Integer type
        //Otherwise, store data as StringValue
        else new Time(TimeScale.JAVA, md, StringValue(value))
        //Note, time scale for text data is assumed to be JAVA, leap second agnostic
        //TODO: use DEFAULT? interpret string as UTC (with leap seconds)? any scale, use when converting to numeric value?
      }
      //No units specified, assume default units
      //TODO: allow specification of Integer type
      //TODO: add units to metadata
      case None => new Time(TimeScale.DEFAULT, md, DoubleValue(value.toDouble)) 
    }
  }
    
//  //def apply(md: Metadata, values: Seq[String]): Time = {
//  //TODO: clean up hack for GranuleAdapter
//  def fromStrings(md: Metadata, values: Seq[String]): Time = {
//    md.get("units") match {
//      case Some(u) => {
//        if (u.contains(" since ")) Time(md, values.map(_.toDouble))
//        else {
//          //change metadata units, immutable
//          //TODO: clean up
//          val props = md.asInstanceOf[VariableMetadata].properties
//          val md2 = Metadata(props + (("units", "milliseconds since 1970-01-01")))
//          val format = new TimeFormat(u)
//          Time(md2, values.map(format.parse(_).getTime().toDouble), TimeScale.DEFAULT)
//        }
//      }
//      case None => Time(md, values.map(_.toDouble))
//    }
//  }
    
  def apply(value: Double): Time = new Time(TimeScale.DEFAULT, data = DoubleValue(value)) //TODO: add Metadata with units
  def apply(value: Long): Time   = new Time(TimeScale.DEFAULT, data = LongValue(value)) //TODO: add Metadata with units
  
  def apply(date: Date): Time = Time(date.getTime())
  
//TODO:
//  val NOW = new Time() {
//    override def getTime() = (new Date()).getTime().toDouble
//  }
}