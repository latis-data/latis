package latis.dm

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter
import latis.writer.Writer
import latis.metadata.Metadata
import latis.ops.filter.Selection
import latis.data.set.IntegerSampledSet
import latis.data.SampledData
import latis.data.IterableData
import latis.data.value.DoubleValue

class TestNestedFunction {

  @Test
  def function_of_functions {
    //val ds = Dataset(TestNestedFunction.function_of_functions)
    val ds = Dataset(TestNestedFunction.function_of_functions_with_sampled_data)
    //val ds2 = Selection("x>1")(ds)
    //val ds2 = Selection("y>=11")(ds)
    AsciiWriter.write(ds)
    //Writer("json").write(ds)
  }
  
  //@Test
  def tuple_of_functions {
    val ds = Dataset(TestNestedFunction.tuple_of_functions)
    AsciiWriter.write(ds) //TODO: improve formatting, 
  }
}

object TestNestedFunction {
    
  def function_of_functions_with_sampled_data = {
    val domain = Integer(Metadata("x"))
    val range = Function(Integer(Metadata("y")), Real(Metadata("z")))
    val dset1 = IntegerSampledSet(List(0,1,2,3))
    val dset2 = IntegerSampledSet(List(10,11,12))
    
    val s1 = SampledData(dset2, IterableData(List(0.0,1.0,2.0).iterator.map(DoubleValue(_)), 8))
    val s2 = SampledData(dset2, IterableData(List(10.0,11.0,12.0).iterator.map(DoubleValue(_)), 8))
    val s3 = SampledData(dset2, IterableData(List(20.0,21.0,22.0).iterator.map(DoubleValue(_)), 8))
    val s4 = SampledData(dset2, IterableData(List(30.0,31.0,32.0).iterator.map(DoubleValue(_)), 8))
    
    val data = SampledData(dset1, IterableData(List(s1,s2,s3,s4).iterator, 32))
    
    Function(domain, range, Metadata("function_of_functions_with_sampled_data"), data)
  }
  
  //TODO: all data in outer Function
  
  def function_of_functions_with_data_in_scalars = {
    val n = 4
    val domain  = (0 until n).map(Integer(Metadata("x"), _))
    val range = for (i <- 0 until n) yield Function((0 until 3).map(j => 
      Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * i + j))))
    Function(domain, range, Metadata("function_of_functions_with_data_in_scalar"))

  }
    
  def tuple_of_functions = {
    val fs = for (i <- 0 until 4) yield Function((0 until 3).map(j =>
      Sample(Integer(Metadata("myInt"+i), 10 + j), Real(Metadata("myReal"+i), 10 * i + j))))
    Tuple(fs, Metadata("tuple_of_functions"))
  }
  
  
}
