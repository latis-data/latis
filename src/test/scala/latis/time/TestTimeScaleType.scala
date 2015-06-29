package latis.time

import org.junit._
import Assert._
import latis.util.LatisProperties
import latis.ops.Operation
import latis.dm.TestDataset
import latis.writer.AsciiWriter
import latis.metadata.Metadata

class TestTimeScaleType {

  @Test 
  def set_default_tai = {
    System.setProperty("time.scale.type", "TAI")
    assertEquals("TAI", LatisProperties("time.scale.type"))
  }
  
  @Test 
  def set_default_utc = {
    System.setProperty("time.scale.type", "UTC")
    assertEquals("UTC", LatisProperties("time.scale.type"))
  }
  
  @Test 
  def convert_time_with_default_utc_time_scale {
    System.setProperty("time.scale.type", "UTC")
    val op = Operation("convert", List("time", "hours since 2000-01-02"))
    val ds = TestDataset.numeric_time_series //days since 2000-01-01
    val ds2 = op(ds)
    val data = ds2.toDoubleMap
    assertEquals(data("myTime").head, -24.0, 0)
  }
  
  @Test 
  def convert_time_with_default_tai_time_scale {
    System.setProperty("time.scale.type", "TAI")
    val op = Operation("convert", List("time", "hours since 2000-01-02"))
    val ds = TestDataset.numeric_time_series //days since 2000-01-01
    val ds2 = op(ds)
    val data = ds2.toDoubleMap
    assertEquals(data("myTime").head, -24.0, 0)
  }
  
  @Test //@Ignore //passes in isolation but fails with others since the defaults are static? Changed defaults from lazy val to def.
  def formatted_time_with_utc_default = {
    //diff should include 1 leap second
    System.setProperty("time.scale.type", "UTC")
    val md = Metadata("units" -> "yyyyMMdd")
    val time1 = Time(md, "20150630")
    val time2 = Time(md, "20150702")
    //convert tp GPS time scale
    val gps = TimeScale("1980-01-06", TimeUnit.MICROSECOND, TimeScaleType.TAI)
    val gps1 = time1.convert(gps).getNumberData.doubleValue
    val gps2 = time2.convert(gps).getNumberData.doubleValue
    val dt = gps2 - gps1
    assertEquals(1119657616000000.0, gps1, 2) //TODO: preserve precision with integer math (LATIS-321)
    assertEquals(1119830417000000.0, gps2, 2)
    assertEquals(1.72801e11, dt, 0.0)
  }
  
  @Test
  def formatted_time_with_tai_default = {
    //both are TAI scales so no leap second diff
    System.setProperty("time.scale.type", "TAI")
    val md = Metadata("units" -> "yyyyMMdd")
    val time1 = Time(md, "20150630")
    val time2 = Time(md, "20150702")
    //convert tp GPS time scale
    val gps = TimeScale("1980-01-06", TimeUnit.MICROSECOND, TimeScaleType.TAI)
    val gps1 = time1.convert(gps).getNumberData.doubleValue
    val gps2 = time2.convert(gps).getNumberData.doubleValue
    val dt = gps2 - gps1
    assertEquals(1.72800e11, dt, 0.0)
  }
  
  @Test
  def formatted_time_with_native_default = {
    //The native scale does not know about leap seconds so just plays along
    val md = Metadata("units" -> "yyyyMMdd")
    val time1 = Time(md, "20150630")
    val time2 = Time(md, "20150702")
    //convert tp GPS time scale
    val gps = TimeScale("1980-01-06", TimeUnit.MICROSECOND, TimeScaleType.TAI)
    val gps1 = time1.convert(gps).getNumberData.doubleValue
    val gps2 = time2.convert(gps).getNumberData.doubleValue
    val dt = gps2 - gps1
    assertEquals(1.72800e11, dt, 0.0)
  }
  
  //TODO: do we need "years without leap days" type?
  
  /*
   * TODO: 
   * 
   * does it ever make sense to have a formatted time have a TAI scale?
   * or should default time.scale.type always be UTC or native?
   * what about user request with num time?
   *   no new Time object made
   *   just compares value with known Time var which could have it's tstype specified
   * 
   * formatted time backed with ms since 1970 but relies on java formatter to convert that to/from formatted time
   * this must be treated as native, ls are not counted
   * but what if we want the time to be treated as UTC?
   *   when converting to tai ???
   *   do we need to convert native to UTC?
   *   
   * ++TimeFormatter
   * convert to java t.s. as UTC or native based on default t.s.t
   * JAVA TimeScale is defined in terms of the default TimeScaleType
   * present l.s. as 60th sec? can't be represented unambiguously in numeric UTC (repeats same value)
   *   only if default time is UTC
   * 
   * ++consider use of things like Time.isoToJava
   * 
   * Use case: MMS WebTCAD
   * - db in gps (TAI), specify tai type in tsml
   * - request in ISO (need default time.scale.type to interp this as UTC)
   * - converting iso as java interpreted as UTC to gps should just work
   * 
   * java and utc time scales have the same numbers
   * only a matter of whether l.s. should be applied
   * so default t.s.t of UTC allows us to use the java time scale but we need to treat it as a UTC scale when converting and computing duration
   * 
   * don't confuse timestamp and duration issues
   *   java time works fine for time stamps but not durations
   *   conversion involves duration between zero-times (epochs)
   * formatting a tai time requires converting to java time as UTC
   * 
   * 
   * ++interpret java time as TAI?
   * if we interp formatted time as TAI then we need to support second 60
   *   java will interpret 60 the same as the next 00
   *   you can represent a leap second in a formatted time, but java will ignore it so we'd have to deal with it
   * 
   *   
   * * js client will always interp java time natively
   * conversion from gps needs to be tai2utc
   * need to add leap seconds to native time values?
   * 
   * ++define t.s.t in tsml
   * 
   * 
   * When does type distinction matter?
   * do we really need 3 types or just "leap seconds yes or no"?
   * Given the constraint that formatted times will be represented as unix numeric time (naive)
   * Formatted times:
   *   only makes sense to interpret as UTC or naive
   *   converting to and from formatted and java time works no matter what - isomorphic?
   *   * define an isomorphic mapping between formatted times and a numeric scale 
   *   java does this for us, at least up to "day" units which is all we can really count on to count real time with a standard unit - invariant (e.g. years may be with or without leap days)
   *   * scale needs to be defined with *invariant* units
   *   interpret that scale as UTC (need to apply leap seconds for durations) or just ignore them (naive) which java does
   *   Doesn't make sense to interpret formatted times as TAI.
   *   we could define ms since 1970 as TAI but it would not map to the correct formatted times
   * Numeric times:
   *   we do need all 3
   *   TAI: all real times are represented, it has counted the leap seconds
   *   UTC: doesn't count leap seconds but we do want to apply them for duration...
   *   NAIVE: doesn't count leap seconds and doesn't care, nor that others do
   *   note, UTC and NAIVE can have the same numerical values (e.g. unix), it's just a matter of how to deal with l.s. in duration
   * 
   * Should we default to UTC
   * adds l.s. dependency - probably enough right there not to, impl as a plugin?
   * affects duration - makes it right but may be inconsistent with other tools, expectations, data...
   * 
   * If type is specified for a dataset, should interpretation of time selection do the same type?
   * probably not, should be uniform for that server instance
   * if ds is naive and format is UCT, conversion will still be naive
   * note, naive latis instance will ignore dataset type
   * 
   * Default type for datasets?
   *   use latis instance default? NO
   *   dangerous to get diff data from diff servers
   *   but UTC vs naive won't change the time values
   *   the time range request offset is a latis impl detail, not a property of the dataset
   *   if the ds is naive, the latis setting won't matter
   *   consider daily dataset: days since xxx
   *     if UTC, then we would have to consider l.s. in duration
   *       problems if user expects ~day precision
   *     if request is UTC: 2015-01-01 ???
   *   * dataset should default to naive
   * 
   * * even if data was labeled with unix time stamps, the scale is naive
   *   unix "accounts for" l.s. while it happens by counting the same second twice, effectively pausing
   *   there is no way to know later that it paused 
   * 
   * Model time as a complex number:
   *   real = unix time
   *   im = number of leap seconds accumulated 
   *   could distinguish a leap second among unix times
   *   could be used for conversion?
   *   but would require converting all times to this form
   * 
   */
}