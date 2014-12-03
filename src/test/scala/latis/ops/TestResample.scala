package latis.ops

import org.junit._
import Assert._
import latis.dm.TestDataset
import latis.writer.AsciiWriter
import latis.data.set.DomainSet
import latis.data.set.RealSampledSet

class TestResample {

  @Test
  def nearest_single_value_equals_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    //(t -> a)
    //0.0 -> 0.0
    //1.0 -> 1.0
    //2.0 -> 2.0
    val set = RealSampledSet(List(1.0))
    val op = new Resample("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("t").length)
    assertEquals(1.0, data("t").head, 0.0)
    assertEquals(1.0, data("a").head, 0.0)
  }

  @Test
  def nearest_single_value_midpoint_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    val set = RealSampledSet(List(1.5))
    val op = new Resample("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("t").length)
    assertEquals(1.5, data("t").head, 0.0)
    assertEquals(2.0, data("a").head, 0.0)
  }
}