package latis.time

import org.junit._
import Assert._
import latis.metadata.Metadata
import latis.dm.Real
import latis.dm.Text
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestLeapSeconds {

  //@Test
  def load {
    val ds = TsmlReader("datasets/test/leap_seconds.tsml").getDataset
    AsciiWriter.write(ds)
  }
  
}