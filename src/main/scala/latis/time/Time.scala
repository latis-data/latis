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
  
  /*
   * 2013-10-24
   * Time needs to be seen as a Scalar.
   * Do we need to have a Scalar impl extend Variable2? with Scalar?
   * Need to revisit all traits as Interfaces with impls.
   * 
   * new Time with Real...
   * doesn't allow us to override compare
   * maybe we should just make: RealTime, IntegerTime,...
   * there must be a better way
   * 
   * TODO: add utc and tai tsml types?
   */
  
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
    case num: NumberData => convert(TimeScale.JAVA).getNumberData.longValue
    case text: TextData => TimeFormat(getMetadata("units")).parse(text.stringValue).getTime
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
  
  def fromIso(s: String): Time = Time(isoToJava(s))
  //TODO: make sure format is valid
  
  def isoToJava(s: String): Long = {
    val cal = javax.xml.bind.DatatypeConverter.parseDateTime(s)
    cal.setTimeZone(TimeZone.getTimeZone("GMT")) //Assume UTC. //TODO: support other time zones?
    cal.getTimeInMillis()
  }
  
  
  //def apply(scale: TimeScale, md: Metadata, data: Data) = new Time(scale, md, data)
  
  //no data, used as a template in adapters
  def apply(md: Metadata): Time = {
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
        case "real" => new Time(scale, md) with Real
        case "integer" => new Time(scale, md) with Integer
        case "text" => new Time(scale, md) with Text
        case _ => throw new RuntimeException("Unsupported Time type: " + s)
      }
      //default to Real
      case None => new Time(scale, md) with Real
    }
  }
  
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
        //Otherwise, store data as StringValue
        else ??? //wait for use case
          //Time(TimeScale.JAVA, md, StringValue(value))
        //TODO: reconcile "units", assumes units = time format string, inconsistent with scale.toString
        /*
         * TODO: what should time scale be for text times?
         *   JAVA: leap second agnostic
         *   UTC: considers leap seconds
         *   allow any scale and transform as requested? seems dangerous
         *   but must be able to get a numeric value at some point, or does that require a "convert"?
         *   consider TimeScale(unit: String)
         */
      }
      //No units specified, assume default numeric units
      //TODO: or if no units, assume formatted? but need format, ISO?
      case None => Time(md, stringToNumber(value)) //TODO: allow specification of type
    }
  }
  
  def fromStrings(md: Metadata, values: Seq[String]): Time = {
    md.get("units") match {
      case Some(u) => {
        if (u.contains(" since ")) Time(md, stringsToNumbers(values))  //numeric units
        else { //formatted time
          //TODO: store as strings, Time with Text
          //convert to java time scale for now, //TODO: use default time scale
          val scale = TimeScale.JAVA
          //make sure units metedata is correct //TODO: add type="integer" to metadata?
          val md2 = Metadata(md.getProperties + ("units" -> scale.toString))
          //parse times into longs
          val format = TimeFormat(u)
          val times: Seq[Long] = values.map(format.parse(_).getTime())
          //note, tempted to delegate to Time(Metadata, Seq[Any]) to get default time scale,
          //  but we are assuming JAVA time here
          new Time(scale, md2, Data(times)) with Integer
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