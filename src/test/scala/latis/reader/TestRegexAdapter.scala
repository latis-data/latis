package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._

class TestRegexAdapter {
  
  @Test
  def log {
    //val ops = List(Operation("last"))
    val ops = List(Selection("time>2014-02-12T18"))
    val ds = TsmlReader("datasets/test/log.tsml").getDataset(ops)
    Writer("csv").write(ds)
  }

}