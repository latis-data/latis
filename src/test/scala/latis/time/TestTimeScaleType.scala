package latis.time

import org.junit._
import Assert._
import latis.util.LatisProperties
import latis.ops.Operation
import latis.dm.TestDataset
import latis.writer.AsciiWriter

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
  
}