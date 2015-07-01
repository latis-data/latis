package latis.reader

import latis.reader.tsml.ml._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestAsciiAdapter  {
  
  val data2 = TsmlReader("datasets/test/data_with_marker.tsml").getDataset
  
  @Test
  def test_data_file_is_readable = {
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset.toStringMap
    assertEquals("1619.5", data("year")(0))
  }
  
  @Test
  def test_only_data_after_marker_is_returned = {
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset.toStringMap
    assertEquals("1619.5", data("year")(0)) // check the fist value
    assertEquals("1628.5", data("year")(9)) // check the last value
  }

}