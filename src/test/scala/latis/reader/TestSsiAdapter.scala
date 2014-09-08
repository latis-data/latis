package latis.reader

import org.junit.Test
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestSsiAdapter {
  
  @Test
  def test {
    val ds = TsmlReader("datasets/test/ssi.tsml").getDataset
    //AsciiWriter.write(ds)
  }

}