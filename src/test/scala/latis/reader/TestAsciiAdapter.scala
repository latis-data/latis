package latis.reader

import latis.reader.tsml.ml._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.writer.Writer

class TestAsciiAdapter  {
  
  @Test
  def test_data_file_is_readable = {
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset.toStringMap
    assertEquals("1619.5", data("year")(0))
  }
  
  @Test
  def test_only_data_after_marker_is_returned = {
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset.toStringMap
    assertEquals("1619.5", data("year")(0)) // check the first value
    assertEquals("1628.5", data("year")(9)) // check the last value
  }
  
  @Test
  def nested {
    val ds = TsmlReader("nested_function.tsml").getDataset
    val data = ds.toDoubleMap
    assertEquals(1.1, data("myReal")(0), 0.0)
    assertEquals(3, data("myTime").length)
    assertEquals(9, data("myText").length)   
    assertEquals(23.3, data("myReal").last, 0.0)
  }

  @Test
  def ascii_ssi = {
    val ds = TsmlReader("datasets/test/ascii_ssi.tsml").getDataset
    AsciiWriter.write(ds)
  }

}
