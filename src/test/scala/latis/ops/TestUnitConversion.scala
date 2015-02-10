package latis.ops

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.dm.TestDataset
import latis.writer.AsciiWriter

class TestUnitConversion {
  
  @Test 
  def convert_numeric_time_units {
    val op = Operation("convert", List("time", "hours since 2000-01-02"))
    val ds = TestDataset.numeric_time_series //days since 2000-01-01
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    assertEquals(data("myTime").head, -24.0, 0)
        
    val tv = ds2.findVariableByName("time").get
    //TODO: assertEquals("hours since 2000-01-02", tv.getMetadata("units").get)
  }
  
  @Test
  def convert_formatted_time_to_numeric_units {
    val op = Operation("convert", List("time", "hours since 1970-01-01"))
    val ds = TestDataset.time_series // yyyy/MM/dd
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    assertEquals(data("myTime")(2), 48.0, 0)
  }
  
  //@Test 
  //TODO: TimeScales don't preserve formatted time, model as units since epoch
  def convert_formatted_time_units {
    val op = Operation("convert", List("time", "yyyy-MM-dd"))
    val ds = TestDataset.time_series // yyyy/MM/dd
    val ds2 = op(ds)
    AsciiWriter.write(ds2)
  }
  
}