package latis.reader

import org.junit.Test
import org.junit.Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.dm.TestDataset

class TestSsiAdapter {
  
  @Test
  def test {
    val ds = TsmlReader("datasets/test/ssi.tsml").getDataset
    assertEquals(TestDataset.function_of_named_scalar, ds)
  }

}
