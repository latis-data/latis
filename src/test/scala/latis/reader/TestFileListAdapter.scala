package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.ops.Selection

class TestFileListAdapter {

  //TODO: make tmp directory structure with files
  
  @Test
  def test {
    //val ops = List(Selection("time>2000-01-01"))
    val ds = TsmlReader("datasets/test/files.tsml").getDataset
    AsciiWriter().write(ds)
  }
}