package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.data.SampledData
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.TestDataset
import latis.dm.implicits.variableToDataset
import latis.metadata.Metadata
import latis.ops.filter.NearestNeighborFilter
import latis.ops.filter.Selection
import latis.reader.tsml.TsmlReader

class TestSelection {
  
  // time -> value
  lazy val scalarFunction: Function = {
    val domain = Real(Metadata("time"))
    val range = Real(Metadata("value"))
    val data = SampledData.fromValues(Seq(1,2,3,4,5), Seq(1,2,3,4,5))
    Function(domain, range, data=data)
  }
  
  //TODO: run same tests against multiple Functions
  def testSelection(f: Function, s: String, expected: Function) {
    val filter = Selection(s)
    val ds = filter(f)
    //AsciiWriter().write(expected)
    //AsciiWriter().write(ds)
    assert(expected equals ds.getVariables(0))
  }

  
  @Test
  def select_equal_value {
    val domain = Real(Metadata("time"))
    val range = Real(Metadata("value"))
    val data = SampledData.fromValues(Seq(3), Seq(3))
    val expected = Function(domain, range, data=data)
    testSelection(scalarFunction, "value=3", expected)
  }
  
  @Test
  def select_gt_value {
    val filter = Selection("value>2")
    val ds = filter(scalarFunction)
    val n = ds.getLength
    assertEquals(3, n)
  }
  
  @Test
  def select_lt_value {
    val filter = Selection("value<3")
    val ds = filter(scalarFunction)
    val n = ds.getLength
    assertEquals(2, n)
  }
  
  @Test
  def select_ge_value {
    val filter = Selection("value>=3")
    val ds = filter(scalarFunction)
    val n = ds.getLength
    assertEquals(3, n)
  }
  
  @Test
  def select_le_value {
    val filter = Selection("value<=3")
    val ds = filter(scalarFunction)
    val n = ds.getLength
    assertEquals(3, n)
  }
  
  @Test
  def select_ne_value {
    val filter = Selection("value!=3")
    val ds = filter(scalarFunction)
    val n = ds.getLength
    assertEquals(4, n)
  }
  
  @Test
  def select_equal_time {
    val filter = Selection("time=3")
    val ds = filter(scalarFunction)
    val n = ds.getLength
    assertEquals(1, n)
  }
  
  @Test
  def select_twice {
    val ds = Dataset(scalarFunction)
    val ds2 = ds.filter("value >= 2")
    val ds3 = ds2.filter("value < 5")
    val n = ds3.getLength
    assertEquals(3, n)
  }
    
  @Test
  def outside_range {
    val ds = Dataset(scalarFunction)
    val ds2 = ds.filter("time > 5")
    val f = ds2.findFunction.get
    assertEquals(0, f.getLength)
  }
    
  //----------------------------------------------
  
  @Test
  def nearest_equals_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    //(t -> a)
    //0.0 -> 0.0
    //1.0 -> 1.0
    //2.0 -> 2.0
    val op = NearestNeighborFilter("t", 1.0)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("t").length)
    assertEquals(1.0, data("t").head, 0.0)
    assertEquals(1.0, data("a").head, 0.0)
  }

  @Test
  def nearest_midpoint_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    val op = NearestNeighborFilter("t", 1.5)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("t").length)
    assertEquals(2.0, data("t").head, 0.0)
    assertEquals(2.0, data("a").head, 0.0)
  }

  @Test
  def nearest_round_down_with_scalar_range {
    val ds = TestDataset.function_of_named_scalar
    val op = NearestNeighborFilter("t", 1.4)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("t").length)
    assertEquals(1.0, data("t").head, 0.0)
    assertEquals(1.0, data("a").head, 0.0)
  }
  
  @Test
  def nearest_from_expression {
    val ds = TestDataset.function_of_named_scalar
    val exp = "t~1.5"
    val op = Selection(exp)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(1, data("t").length)
    assertEquals(2.0, data("t").head, 0.0)
    assertEquals(2.0, data("a").head, 0.0)
  }
  
  @Test
  def nearest_equal_last_sample_with_int_domain_tuple_range {
    val ds = TestDataset.function_of_tuple
    //(myInteger -> (myReal, myText))
    //0 -> (0.0, zero)
    //1 -> (1.0, one)
    //2 -> (2.0, two)
    val op = NearestNeighborFilter("myInteger", 2)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    
    assertEquals(1, data("myInteger").length)
    assertEquals("2", data("myInteger").head)
    assertEquals("2.0", data("myReal").head)
    assertEquals("two", data("myText").head)
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
  def nearest_with_function_range {
    val ds = TestDataset.function_of_functions2
    val op = NearestNeighborFilter("x", 2)
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
    val op = NearestNeighborFilter("y", 11)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    
    assertEquals(4, data("x").length)
    assertEquals(4, data("y").length) //1 for each outer sample
    assertEquals(11.0, data("y").head, 0.0)
    assertEquals(1.0, data("z").head, 0.0)
  }

  @Test
  def nearest_nested_function_sample_from_expression {
    val ds = TestDataset.function_of_functions2
    val exp = "y~11"
    val op = Selection(exp)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap

    assertEquals(4, data("x").length)
    assertEquals(4, data("y").length) //1 for each outer sample
    assertEquals(11.0, data("y").head, 0.0)
    assertEquals(1.0, data("z").head, 0.0)
  }
 
  @Test
  def nearest_equals_text_time_domain {
    //TODO: Time as integer vs real
    val ds = TestDataset.time_series
    val op = NearestNeighborFilter("time", "1970-01-02")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myTime").length)
    assertEquals("1970/01/02", data("myTime").head)
    assertEquals("2.2", data("myReal").head)
  }
 
  @Test
  def nearest_midpoint_text_time_domain {
    //TODO: Time as integer vs real
    val ds = TestDataset.time_series
    val op = NearestNeighborFilter("time", "1970-01-01T12:00:00")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myTime").length)
    assertEquals("1970/01/02", data("myTime").head)
    assertEquals("2.2", data("myReal").head)
  }
  
  @Test
  def nearest_iso_equals_real_time_domain {
    val ds = TestDataset.numeric_time_series
    val op = NearestNeighborFilter("time", "2000-01-01")
    val ds2 = op(ds)
    // AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    assertEquals(1, data("myTime").length)
    assertEquals(0.0, data("myTime").head, 0.0)
    assertEquals(1.1, data("myReal").head, 0.0)
  }
  
  @Test
  def nearest_iso_midpoint_real_time_domain {
    val ds = TestDataset.numeric_time_series
    val op = NearestNeighborFilter("time", "2000-01-01T12:00:00")
    val ds2 = op(ds)
    // AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    assertEquals(1, data("myTime").length)
    assertEquals(1.0, data("myTime").head, 0.0)
    assertEquals(2.2, data("myReal").head, 0.0)
  }
  
  @Test
  def nearest_numeric_equals_real_time_domain {
    val ds = TestDataset.numeric_time_series
    val op = NearestNeighborFilter("time", "0")
    val ds2 = op(ds)
    // AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    assertEquals(1, data("myTime").length)
    assertEquals(0.0, data("myTime").head, 0.0)
    assertEquals(1.1, data("myReal").head, 0.0)
  }
  
  @Test
  def nearest_numeric_midpoint_real_time_domain {
    val ds = TestDataset.numeric_time_series
    val op = NearestNeighborFilter("time", "1.5")
    val ds2 = op(ds)
    // AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    assertEquals(1, data("myTime").length)
    assertEquals(2.0, data("myTime").head, 0.0)
    assertEquals(3.3, data("myReal").head, 0.0)
  }
  
  //TODO: error if no match for domain name
  
  @Test
  def before_valid_range {
    val ds = TestDataset.function_of_named_scalar
    val op = NearestNeighborFilter("t", -1.5)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toDoubleMap
    //TODO: ds2 function's sampled data still has domain set with requested sample, which is used by ds.getLength
    assert(data.isEmpty)
  }
  
  @Test
  def after_valid_range {
    val ds = TestDataset.function_of_named_scalar
    val op = NearestNeighborFilter("t", 4.5)
    val ds2 = op(ds)
    val data = ds2.toDoubleMap
    assert(data.isEmpty)
  }
  
  @Test
  def index_reset {
    val tr = TsmlReader("datasets/test/index.tsml")
    val ops = Seq(Selection("time>1970-01-01"))
    val ds = tr.getDataset(ops)
//    AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(data("index")(1), 1.0, 0.0)
    
    
  }
}




