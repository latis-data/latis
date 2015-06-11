package latis.ops

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.dm.TestDataset
import latis.writer.AsciiWriter

class TestTimeFormatter {
  
  @Test 
  def format_numeric_time {
    val op = TimeFormatter("yyyy-MM-dd")
    val ds = TestDataset.numeric_time_series //days since 2000-01-01
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(data("myTime").length, 3) //didn't change number of samples
    assertEquals(data("myTime").head, "2000-01-01")
    
    val tv = ds2.findVariableByName("time").get
    val units = tv.getMetadata("units").get
    assertEquals("yyyy-MM-dd", units)
  }
  
  @Test
  def format_formatted_time {
    val op = TimeFormatter("yyyy-MM-dd")
    val ds = TestDataset.time_series // yyyy/MM/dd
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(data("myTime").length, 3) //didn't change number of samples
    assertEquals(data("myTime")(1), "1970-01-02")
        
    val units = ds2.findVariableByName("time").get.getMetadata("units").get
    assertEquals("yyyy-MM-dd", units)
  }
  
  @Test
  def format_with_expression {
    val op = Operation("format_time", List("yyyy-MM-dd"))
    val ds = TestDataset.time_series // yyyy/MM/dd
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(data("myTime").length, 3) //didn't change number of samples
    assertEquals(data("myTime")(1), "1970-01-02")
  }
  
  @Test
  def format_with_time_component_separator {
    //Note, quotes required around T
    val op = Operation("format_time", List("yyyy-MM-dd'T'HH:mm:ss"))
    val ds = TestDataset.time_series // yyyy/MM/dd
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(data("myTime").length, 3) //didn't change number of samples
    assertEquals(data("myTime")(1), "1970-01-02T00:00:00")
  }
  
}