package latis.writer

import org.junit._
import latis.reader.tsml.TsmlReader
import latis.ops.Selection

class TestHtmlWriter {
  
  //@Test
  def write_html {
    val reader = TsmlReader("datasets/test/composite_lyman_alpha.tsml")
    val ds = reader.getDataset()
    Writer.fromSuffix("html").write(ds)
  }

}