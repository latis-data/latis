package latis.ops

import org.junit._
import Assert._
import latis.dm.Dataset
import latis.dm.Naught
import latis.dm.Function
import latis.dm.TestDataset
import latis.ops.filter.ExcludeMissing
import latis.writer.AsciiWriter
import latis.dm.Real
import latis.metadata.Metadata
import latis.data.SampledData
import latis.dm.TestFunction
import latis.writer.Writer

class TestExcludeMissing {

  @Test 
  def no_missing_values {
    val ds = ExcludeMissing()(TestDataset.function_of_scalar)
    assertEquals(3, ds.getLength)
  }
  
  @Test
  def missing_scalar_function_with_nan {
    val ds = ExcludeMissing()(TestDataset.function_of_scalar_with_nan)
    assertEquals(2, ds.getLength)
  }
  
  //no empty datasets?
//  @Test
//  def missing_scalar {
//    val ds = ExcludeMissing()(TestDataset.nan)
//    val vars = ds.getVariables
//    assertEquals(0, vars.length)
//    //AsciiWriter.write(ds)
//  }
//  
//  @Test
//  def missing_in_tuple {
//    val ds = ExcludeMissing()(TestDataset.tuple_with_nan)
//    val vars = ds.getVariables
//    assertEquals(0, vars.length)
//  }
  
  @Test
  def missing_tuple_in_function = {
    val ds = ExcludeMissing()(TestDataset.function_of_tuple_with_nan)
    assertEquals(2, ds.getLength)
  }
  
  @Test
  def missing_in_domain = {
    val ds = ExcludeMissing()(TestDataset.function_of_scalar_with_nan_in_domain)
    assertEquals(2, ds.getLength)
  }
  
  @Test
  def missing_scalar_function_with_defined_missing_value = {
    val domain = Real(Metadata("name" -> "foo"))
    val range = Real(Metadata("missing_value" -> "0"))
    val data = SampledData.fromValues(List(0,1,2), List(3,0,5))
    val ds = Dataset(Function(domain, range, data = data))
    val ds2 = ExcludeMissing()(ds)
    assertEquals(2, ds2.getLength)
  }
  
  @Test
  def exclude_missing_from_empty_dataset {
    val op = Operation("exclude_missing")
    val ds = TestFunction.empty_function
    val ds2 = op(ds)
    //Writer.fromSuffix("jsond").write(ds2)
    val data = ds2.toStringMap
    assert(data.isEmpty)
  }
  
  def missing_in_nested_function = ???
}
