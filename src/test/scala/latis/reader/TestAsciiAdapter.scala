package latis.reader

import latis.reader.tsml.ml._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestAsciiAdapter  {
    
  val data2 = TsmlReader("datasets/test/data_with_marker.tsml").getDataset
  
  @Test
  def test_read_data_file = {
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset.toStringMap
    assertEquals("1610.5", data("year")(0))
  }
  

}