package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestRegexAdapter {
  
  @Test
  def log {
    val ds = TsmlReader("datasets/test/log.tsml").getDataset
    AsciiWriter().write(ds)
  }

}