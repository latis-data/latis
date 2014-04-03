package latis.writer

import org.junit._
import latis.reader.tsml.TsmlReader
import latis.ops.Selection

class TestHtmlWriter {
  
  @Test
  def write_html {
    val reader = TsmlReader("datasets/test/tsi.tsml")
    val ds = reader.getDataset()
    Writer.fromSuffix("html").write(ds)
  }

}