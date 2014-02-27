package latis.writer

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader

class TestImageWriter {

  @Test
  def test{
    val ds = TsmlReader("datasets/test/tsi.tsml").getDataset
    Writer("image").write(ds)
  }

}