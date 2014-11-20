package latis.ops

import latis.dm.TestDataset
import latis.writer.AsciiWriter

class TestIntegrate {
  
  //@Test
  def test {
    val op = new Integrate()
    val ds = TestDataset.function_of_functions
    AsciiWriter.write(op(ds))
  }

}