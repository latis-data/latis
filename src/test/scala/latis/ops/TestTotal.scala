package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import latis.dm.TestDataset
import latis.writer.AsciiWriter
import latis.dm.Dataset
import scala.collection.mutable.ArrayBuffer
import latis.ops.filter.Selection

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
  def project_canonical {
    val ds = Total()(Projection(Seq("myTime", "myInt", "myReal"))(TestDataset.canonical))
    val data = ds.toStrings
    assertEquals(1, data(0).length)
    assertEquals("6.6", data(2)(0))
    assertEquals("6.0", data(1)(0))
    assertEquals("1970/01/03", data(0)(0))
  }
  
  @Test
  def project_with_selection_without_domain {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("myInt,myReal")
    ops += Selection("myInt>1")
    ops += Total()
    val ds = TestDataset.canonical
    val ds2 = ops.foldLeft(ds)((dataset, op) => op(dataset))
    val data = ds2.toDoubleMap
    assertEquals(1, data("myInt").length)
    assertEquals(5.0, data("myInt")(0), 0.0)
    assertEquals(5.5, data("myReal")(0), 0.0)
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
  
  @Test
  def empty {
    val ds = Total()(TestDataset.empty_function)
    val data = ds.toStrings
    assert(data.isEmpty)
  }

}