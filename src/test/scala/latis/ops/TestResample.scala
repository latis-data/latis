package latis.ops

import org.junit._
import Assert._
import latis.dm.TestDataset
import latis.writer.AsciiWriter
import latis.data.set.DomainSet
import latis.data.set.RealSampledSet
import latis.ops.resample.NearestNeighbor
import latis.data.set.IntegerSampledSet
import latis.time.Time
import latis.data.set.TextSampledSet
import latis.ops.filter.Selection

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
  def nearest_two_values_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    val set = RealSampledSet(List(0.5,1.5))
    val op = new NearestNeighbor("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(2, data("t").length)
    assertEquals(0.5, data("t").head, 0.0)
    assertEquals(1.0, data("a").head, 0.0)
    assertEquals(1.5, data("t")(1), 0.0)
    assertEquals(2.0, data("a")(1), 0.0)
  }

  @Test
  def nearest_two_values_same_sample_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    val set = RealSampledSet(List(0.5,1.2))
    val op = new NearestNeighbor("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(2, data("t").length)
    assertEquals(0.5, data("t").head, 0.0)
    assertEquals(1.0, data("a").head, 0.0)
    assertEquals(1.2, data("t")(1), 0.0)
    assertEquals(1.0, data("a")(1), 0.0)
  }

  @Test
  def nearest_3_values_invalid_after_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    val set = RealSampledSet(List(0.5, 1.5, 2.3))
    val op = new NearestNeighbor("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(2, data("t").length)
    assertEquals(0.5, data("t").head, 0.0)
    assertEquals(1.0, data("a").head, 0.0)
    assertEquals(1.5, data("t")(1), 0.0)
    assertEquals(2.0, data("a")(1), 0.0)
  }

  @Test
  def nearest_3_values_invalid_before_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    val set = RealSampledSet(List(-1.1, 0.5, 1.5))
    val op = new NearestNeighbor("t", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(2, data("t").length)
    assertEquals(0.5, data("t").head, 0.0)
    assertEquals(1.0, data("a").head, 0.0)
    assertEquals(1.5, data("t")(1), 0.0)
    assertEquals(2.0, data("a")(1), 0.0)
  }

  //TODO: set out of order
  
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

  @Test
  def nearest_single_value_with_function_range {
    val ds = TestDataset.function_of_functions2
    val set = IntegerSampledSet(List(2))
    val op = new NearestNeighbor("x", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("x").length)
    assertEquals(3, data("y").length)
    assertEquals(2.0, data("x").head, 0.0)
    assertEquals(20.0, data("z").head, 0.0)
  }

  @Test
  def nearest_nested_function_sample {
    val ds = TestDataset.function_of_functions2
    //AsciiWriter.write(ds)
    val set = IntegerSampledSet(List(11))
    val op = new NearestNeighbor("y", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap

    assertEquals(4, data("x").length)
    assertEquals(4, data("y").length) //1 for each outer sample
    assertEquals(11.0, data("y").head, 0.0)
    assertEquals(1.0, data("z").head, 0.0)
  }
 
//  function_of_functions: (x -> y -> z)
//0 -> 10 -> 0.0
//     11 -> 1.0
//     12 -> 2.0
//1 -> 10 -> 10.0
//     11 -> 11.0
//     12 -> 12.0
//2 -> 10 -> 20.0
//     11 -> 21.0
//     12 -> 22.0
//3 -> 10 -> 30.0
//     11 -> 31.0
//     12 -> 32.0
  
  
  @Test
  def nearest_single_value_equals_time_domain {
    //TODO: Time as integer vs real
    val ds = TestDataset.time_series
    val format = ds.unwrap.findVariableByName("time").get.getMetadata("units").get
    val newTime = Time.fromIso("1970-01-02")
    val t = newTime.format(format)
    /*
     * TODO: set type needs to match domain type, Time as Text in this case
     * use TimeSet or TextSet?
     * consider ordering: use TextSet only if lexically ordered
     * need TimeSet here
     * should TimeSet always use Doubles? convert as needed
     * 
     * e.g. domain = Time as Text
     * converted to java time double for comparison
     * Data in DomainSet is applied to orig Time variable (text type, formatted units)
     *   time(data)
     * TextSet might 'just work'
     * must use native format
     */
    //val set = IntegerSampledSet(List(time.getJavaTime))
    
    //val set = TextSampledSet(List("1970-01-02"))
    //val set = TextSampledSet(List("1970/01/02"))
    val set = TextSampledSet(List(t))
    val op = new NearestNeighbor("time", set)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myTime").length)
    assertEquals("1970/01/02", data("myTime").head)
    assertEquals("2.2", data("myReal").head)
    
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