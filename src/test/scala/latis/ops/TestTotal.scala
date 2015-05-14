package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import latis.dm.TestDataset
import latis.writer.AsciiWriter
import latis.dm.Dataset

class TestTotal {
  
  @Test
  def canonical {
    try{
      val ds = Total()(TestDataset.canonical)
      fail("Total should have failed on Text Variable")
    }
    catch{
      case _: UnsupportedOperationException => //test passed
    }
  }
  
  @Test
  def function_of_scalars {
    val ds = Total()(TestDataset.function_of_named_scalar)
    val data = ds.toStrings
    assertEquals(2, data.length)
    assertEquals(1, data(0).length)
    assertEquals("2.0", data(0)(0))
    assertEquals("3.0", data(1)(0))
  }

}