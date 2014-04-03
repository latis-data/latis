package latis.writer

import org.junit._
import latis.reader.tsml.TsmlReader
import latis.ops.Selection

class TestInfoWriter {
  
  //@Test
  def write_info {
    val reader = TsmlReader("datasets/test/lemr.tsml")
    val ds = reader.getDataset()
    Writer.fromSuffix("info").write(ds)
  }

}