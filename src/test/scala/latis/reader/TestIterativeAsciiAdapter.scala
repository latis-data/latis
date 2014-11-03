package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestIterativeAsciiAdapter extends AdapterTests {
  def datasetName = "ascii_iterative"
    
  @Test
  def float_as_int {
    val ds = TsmlReader("ascii_float_as_int.tsml").getDataset
    val data = ds.toDoubleMap
    assertEquals(1.0, data("myReal").head, 0.0)
  }
}