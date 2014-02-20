package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestColumnarAdapter {

  @Test
  def test {
    val ds = TsmlReader("datasets/test/col.tsml").getDataset
    AsciiWriter().write(ds)
  }
}