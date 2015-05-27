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
  def select_outer_domain {
    val ds = Dataset(TestNestedFunction.function_of_functions_with_sampled_data)
    val ds2 = Selection("x>1")(ds)
    assertEquals(2, ds2.getLength)
  }
  
  @Test
  def select_inner_domain {
    val ds = Dataset(TestNestedFunction.function_of_functions_with_sampled_data)
    val ds2 = Selection("y>10")(ds)
    val f = ds2.unwrap.asInstanceOf[Function].iterator.next.asInstanceOf[Sample].range.asInstanceOf[Function]
    val n = f.getLength
    assertEquals(3, n)
  }
  
  @Test
  def select_inner_range {
    val ds = Dataset(TestNestedFunction.function_of_functions_with_sampled_data)
    val ds2 = Selection("z>20")(ds)
    val dataMaps = ds2.unwrap.asInstanceOf[Function].iterator.map(_.range.asInstanceOf[Function]).toList.map(Dataset(_).toDoubleMap)
    assertEquals(4, dataMaps.length) //all original inner functions still exist (change in behavior after 2.8.3)
    //TODO: currently returning only matching samples, not complete spectrum, but md has orig length
    assert(dataMaps(1).isEmpty) 
    assertEquals(2, dataMaps(2)("y").length)
    assertEquals(3, dataMaps(3)("y").length)
//=======
//    val fs = ds2.unwrap.asInstanceOf[Function].iterator.map(_.range.asInstanceOf[Function]).toList
//    assertEquals(2, fs.length)
//    //AsciiWriter.write(Dataset(fs(0)))
//    //assertEquals(2, fs(0).getLength) //TODO: currently returning only matching samples, not complete spectrum, but md has orig length
//    assertEquals(3, fs(1).getLength)
//>>>>>>> refs/heads/dataset-not-tuple
  }
  
  //@Test
  def function_of_functions {
    //val ds = Dataset(TestNestedFunction.function_of_functions)
    val ds = Dataset(TestNestedFunction.function_of_functions_with_sampled_data)
    //val ds2 = Selection("x>1")(ds)
    //val ds2 = Selection("y>=11")(ds)
    //val ds2 = ds.filter("y>=11").filter("x>1")
    //val ds2 = ds.filter("x>1").filter("y>=11")
    val ds2 = ds.filter("z>20") //TODO: doesn't drop entire x sample
    AsciiWriter.write(ds2)
    //Writer.fromSuffix("jsond").write(ds)
  }
  
  //@Test
  def tuple_of_functions {
    val ds = Dataset(TestNestedFunction.tuple_of_functions)
    AsciiWriter.write(ds) //TODO: improve formatting, 
  }
}

object TestNestedFunction {
  
  //TODO: easier way to construct Function
  //TODO: Tuple in range of nested function
  //TODO: triple nested function, t -> (lon,lat) -> wl -> (a,b,c)
    
  def function_of_functions_with_sampled_data = {
    val domain = Integer(Metadata("x"))
    val range = Function(Integer(Metadata("y")), Real(Metadata("z")), Metadata(Map("length" -> "3")))
    //TODO: Function construction should add "length" to metadata
    
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
  
  def function_of_functions_with_tuple_range = {
    val n = 4
    val m = 3
    val domain  = (0 until n).map(Real(Metadata("t"), _))
    val range = for (i <- 0 until n) yield Function((0 until m).map(j => 
      Sample(Real(Metadata("w"), 10 + j), Tuple(Real(Metadata("a"), 10 * i + j), Real(Metadata("b"), 10 * i + j)) )), 
        Metadata(Map("length" -> m.toString)))
    Function(domain, range, Metadata(Map("name" -> "function_of_functions_with_tuple_range", "length" -> n.toString)))
  }
    
  def tuple_of_functions = {
    val fs = for (i <- 0 until 4) yield Function((0 until 3).map(j =>
      Sample(Integer(Metadata("myInt"+i), 10 + j), Real(Metadata("myReal"+i), 10 * i + j))))
    Tuple(fs, Metadata("tuple_of_functions"))
  }
  
  
}
