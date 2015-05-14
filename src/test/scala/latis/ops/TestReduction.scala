package latis.ops

import latis.dm._
import latis.ops.Reduction._
import latis.writer.AsciiWriter

import scala.collection._
import scala.collection.mutable.ArrayBuffer

import org.junit._
import Assert._

class TestReduction {

  @Test
  def scalar {
    val ds = Dataset(TestReal.pi)
    val ds2 = reduce(ds)
    assertTrue(ds2.unwrap.isInstanceOf[Real])
  }

  @Test
  def tuple_of_one {
    val ds = Dataset(TestTuple.tuple_of_real)
    val ds2 = reduce(ds)
    assertTrue(ds2.unwrap.isInstanceOf[Real])
  }

  @Test
  def tuple_of_two {
    val ds = Dataset(TestTuple.tuple_of_reals)
    val ds2 = reduce(ds)
    val v = ds2.unwrap
    assertTrue(v.isInstanceOf[Tuple])
    assertEquals(2, v.asInstanceOf[Tuple].getVariables.length)
  }
  
  @Test
  def nested_tuples = {
    val ds = TestDataset.tuple_of_tuples
    val ds2 = ds.reduce
    assertEquals(2, ds.unwrap.asInstanceOf[Tuple].getElementCount)
    assertEquals(4, ds2.unwrap.asInstanceOf[Tuple].getElementCount)
  }

//  @Test
//  def function_of_one {
//    val ds = Dataset(TestFunction.function_with_one_sample_of_scalar_with_data_from_kids)
//    val ds2 = reduce(ds)
//    val v = ds2.getVariables.head
//    assertTrue(v.isInstanceOf[Sample])
//  }
//  
//  @Test
//  def function_of_many {
//    val ds = Dataset(TestFunction.function_of_scalar_with_data_from_kids)
//    val ds2 = reduce(ds)
//    val v = ds2.getVariables.head
//    assertTrue(v.isInstanceOf[Function])
//  }
  
  //TODO: need to define valid dataset @Test
  def iterable_function_of_one {
    val ds = Dataset(TestFunction.function_with_one_sample_of_scalar_with_iterable_data)
    val ds2 = reduce(ds)
    val v = ds2.unwrap
    assertTrue(v.isInstanceOf[Sample])
  }
  
  //TODO: need to define valid dataset @Test
  def iterable_function_of_many {
    val ds = Dataset(TestFunction.function_of_scalar_with_iterable_data)
    val ds2 = reduce(ds)
    val v = ds2.unwrap
    assertTrue(v.isInstanceOf[Function])
  }
  
  //TODO: tuple_of_one_tuple_of_one_scalar
  //TODO: tuple_of_one_function_with_one_sample
  //TODO: function_with_one_sample_of_tuple_of_one_scalar
  //TODO: function_with_one_sample_of_function_with_one_sample
  //TODO: 2D function
}