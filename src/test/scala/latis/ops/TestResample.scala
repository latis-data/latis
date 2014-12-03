package latis.ops

import org.junit._
import Assert._
import latis.dm.TestDataset
import latis.writer.AsciiWriter
import latis.data.set.DomainSet
import latis.data.set.RealSampledSet
import latis.ops.resample.NearestNeighbor
import latis.data.set.IntegerSampledSet

class TestResample {

  @Test
  def nearest_single_value_equals_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    //(t -> a)
    //0.0 -> 0.0
    //1.0 -> 1.0
    //2.0 -> 2.0
    val set = RealSampledSet(List(1.0))
    val op = new NearestNeighbor("t", set)
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
    val op = new NearestNeighbor("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("t").length)
    assertEquals(1.5, data("t").head, 0.0)
    assertEquals(2.0, data("a").head, 0.0)
  }

  @Test
  def nearest_single_value_midpoint_with_int_domain_tuple_range {
    val ds = TestDataset.function_of_tuple
    //(myInteger -> (myReal, myText))
    //0 -> (0.0, zero)
    //1 -> (1.0, one)
    //2 -> (2.0, two)
    val set = IntegerSampledSet(List(2))
    val op = new NearestNeighbor("myInteger", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    
    assertEquals(1, data("myInteger").length)
    assertEquals("2", data("myInteger").head)
    assertEquals("2.0", data("myReal").head)
    assertEquals("two", data("myText").head)
  }
  
  //TODO: error if no match for domain name
  
  @Test
  def before_valid_range {
    val ds = TestDataset.function_of_named_scalar
    val set = RealSampledSet(List(-1.5))
    val op = new NearestNeighbor("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    //TODO: ds2 function's sampled data still has domain set with requested sample, which is used by ds.getLength
    assert(data.isEmpty)
  }
  
  @Test
  def after_valid_range {
    val ds = TestDataset.function_of_named_scalar
    val set = RealSampledSet(List(4.5))
    val op = new NearestNeighbor("t", set)
    val ds2 = op(ds)
    val data = ds2.toDoubleMap
    assert(data.isEmpty)
  }
}