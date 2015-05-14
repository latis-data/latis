package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.dm.TestDataset
import latis.writer.AsciiWriter

class TestSum {
  
  @Test
  def canonical {
    val ds = Sum()(TestDataset.canonical)
    val data = ds.toStrings
    assertEquals(1, data(0).length)
    assertEquals("6.6", data(2)(0))
    assertEquals("C", data(3)(0))
    assertEquals("1970/01/03", data(0)(0))
  }
  
  @Test
  def function_of_scalars {
    val ds = Sum()(TestDataset.function_of_named_scalar)
    val data = ds.toStrings
    assertEquals(2, data.length)
    assertEquals(1, data(0).length)
    assertEquals("2.0", data(0)(0))
    assertEquals("3.0", data(1)(0))
  }

}