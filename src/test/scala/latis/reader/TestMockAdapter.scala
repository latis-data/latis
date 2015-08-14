package latis.reader

import latis.writer.AsciiWriter
import latis.reader.tsml.TsmlReader
import org.junit.Test

class TestMockAdapter {
  
//  @Test
  def test {
    val ds = TsmlReader("mock.tsml").getDataset
    AsciiWriter.write(ds)
  }
  
}