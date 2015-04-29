package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter
import latis.reader.tsml.TsmlReader
import latis.ops.filter.Selection
import latis.ops.filter.LastFilter
import latis.ops.filter.StrideFilter

class TestFunction {
  
  //TODO: reexamine test Functions now that we no longer 'iterate from kids'
  //@Test
  def getLength_reiterable {
    val a = TestFunction.function_of_scalar_with_iterable_data
    assertEquals(3, a.getLength)
    assertEquals(3, a.getLength)
  }
  //@Test
  //def function_of_scalar_with_data_from_kids = AsciiWriter.write(TestFunction.function_of_scalar_with_data_from_kids)
  //def function_of_scalar_with_iterable_data = AsciiWriter.write(TestFunction.function_of_scalar_with_iterable_data)
  //def function_of_tuple_with_data_from_kids = AsciiWriter.write(TestFunction.function_of_tuple_with_data_from_kids)
  //def function_of_tuple_with_iterable_data = AsciiWriter.write(TestFunction.function_of_tuple_with_iterable_data)
  //def function_of_tuple_with_mixed_types = AsciiWriter.write(TestFunction.function_of_tuple_with_mixed_types)
  @Test
  def reset_index_after_selection {
    val tr = TsmlReader("datasets/test/index.tsml")
    val ops = Seq(Selection("time>1970-01-01"))
    val ds = tr.getDataset(ops)
//    AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(data("index")(1), 1.0, 0.0)
  }
  
  @Test
  def reset_index_after_lastfilter {
    val tr = TsmlReader("datasets/test/index.tsml")
    val ops = Seq(LastFilter())
    val ds = tr.getDataset(ops)
//    AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(data("index")(0), 0.0, 0.0)
  }
  
  @Test
  def reset_index_after_stridefilter {
    val tr = TsmlReader("datasets/test/index.tsml")
    val ops = Seq(StrideFilter(2))
    val ds = tr.getDataset(ops)
//    AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(data("index")(1), 1.0, 0.0)
  }
  
  
  
//  @Test(expected = classOf[Error])
//  def iterate_twice_error {
//    val f = TestFunction.function_of_scalar_with_iterable_data
//    val n1 = f.iterator.toList.length
//    val n2 = f.iterator.toList.length
//  }
}

object TestFunction {

//  def function_with_one_sample_of_scalar_with_data_from_kids = {
//    Function.fromValues(List(List(0.0), List(0.0)))
//  }
//
//  def function_of_scalar_with_data_from_kids = {
//    Function.fromValues(List(List(0.0, 1.0, 2.0), List(0.0, 1.0, 2.0)))
//  }

  def function_with_one_sample_of_scalar_with_iterable_data = {
    val samples = List(Sample(Real(0), Real(0)))
    Function(samples)
  }

  def function_of_scalar_with_iterable_data = {
    val samples = List(Sample(Real(0), Real(0)), 
                       Sample(Real(1), Real(1)), 
                       Sample(Real(2), Real(2)))
    Function(samples)
  }

//  def function_with_one_sample_of_tuple_with_data_from_kids = {
//    Function.fromValues(List(List(0.0), List(0.0), List(10.0)))
//  }
//
//  def function_of_tuple_with_data_from_kids = {
//    Function.fromValues(List(List(0.0, 1.0, 2.0), List(0.0, 1.0, 2.0), List(10.0, 11.0, 12.0)))
//  }

  def function_of_tuple_with_iterable_data = {
    val samples = List(Sample(Real(0), Tuple(Real(0), Real(10))), 
                       Sample(Real(1), Tuple(Real(1), Real(11))), 
                       Sample(Real(2), Tuple(Real(2), Real(12))))
    Function(samples)
  }
  
  def function_of_tuple_with_mixed_types = {
    val samples = List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero"))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 1), Text(Metadata("myText"), "one"))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two"))))
    Function(samples)
  }
  
  def empty_function = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty))
  
  //def index_function = Dataset(Function(List(Text("msg", "Hi"), Text("msg", "Bye")), "myIndexFunction"), "indexFunctionDS")
  def index_function = Function(List(Integer(1), Integer(2)))
  
}