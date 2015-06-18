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
  
  @Test @Ignore //passes in isolation but fails with others since the defaults are static (LATIS-290)
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
  
  /*
   * TODO: 
   * 
   * does it ever make sense to has a formatted time have a TAI scale?
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
   * TimeFormatter
   * does it need to care?
   */
}