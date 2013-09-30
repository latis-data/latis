package latis.time

import latis.dm.Real
import java.util.Date
import latis.metadata.Metadata
import latis.data.value.DoubleValue
import latis.data.EmptyData
import latis.data.Data
import latis.metadata.EmptyMetadata
import latis.util.RegEx
import latis.metadata.VariableMetadata
import java.util.TimeZone

class Time extends Real {
  
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
        //Assume UTC. //TODO: support other time zones?
        val cal = javax.xml.bind.DatatypeConverter.parseDateTime(s)
        cal.setTimeZone(TimeZone.getTimeZone("GMT"))
        val t = Time(cal.getTimeInMillis().toDouble)
        //val t = Time(javax.xml.bind.DatatypeConverter.parseDateTime(s).getTimeInMillis().toDouble)
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
    val scale = md.get("units") match {
      case Some(u) => TimeScale(u)
      case None => TimeScale.JAVA
    }
    val t = new Time
    t._metadata = md
    t._data = Data(values)
    t.timeScale = scale
    t
  }
    
  def apply(md: Metadata, values: Seq[Double], scale: TimeScale): Time = {
    val t = new Time
    t._metadata = md
    t._data = Data(values)
    t.timeScale = scale
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
     *     
     * should format in md suggest that binary have string representation?
     *   consider idl api where user wants yyyyDDD, otherwise need converter from unix time
     * do we store string in Time Data?
     *   Time extends Real
     *   but could always behave by supplying double
     *   
     * What about Integer time?
     *   avoid round off problems
     * Should Time extend Scalar and support double, long, or String?
     *   Real, Integer, Text as traits?
     *   
     */
    md.get("units") match {
      case Some(u) => {
        if (u.contains(" since ")) Time(md, value.toDouble, TimeScale.JAVA)
        else {
          val format = new TimeFormat(u)
          Time(md, format.parse(value).getTime(), TimeScale.JAVA)
          //TODO: change units 
          //TODO: consider units and format attributes
        }
      }
      case None => Time(md, value.toDouble, TimeScale.JAVA) //assume default units
        //throw new RuntimeException("Time Metadata is missing units.")
    }
  }
    
  //def apply(md: Metadata, values: Seq[String]): Time = {
  //TODO: clean up hack for GranuleAdapter
  def fromStrings(md: Metadata, values: Seq[String]): Time = {
    md.get("units") match {
      case Some(u) => {
        if (u.contains(" since ")) Time(md, values.map(_.toDouble))
        else {
          //change metadata units, immutable
          //TODO: clean up
          val props = md.asInstanceOf[VariableMetadata].properties
          val md2 = Metadata(props + (("units", "milliseconds since 1970-01-01")))
          val format = new TimeFormat(u)
          Time(md2, values.map(format.parse(_).getTime().toDouble), TimeScale.JAVA)
        }
      }
      case None => Time(md, values.map(_.toDouble))
    }
  }
    
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