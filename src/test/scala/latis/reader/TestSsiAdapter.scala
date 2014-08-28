package latis.reader

import org.junit.Test
import latis.reader.tsml.TsmlReader

class TestSsiAdapter {
  
  @Test
  def test {
    val ds = TsmlReader("src/test/resources/datasets/test/ssi.tsml").getDataset
  }

}