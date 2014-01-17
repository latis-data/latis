package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter

class TestDataset {
  
  @Test
  //def function_of_scalar_with_data_from_kids = AsciiWriter.write(TestDataset.function_of_scalar_with_data_from_kids)
  //def function_of_scalar_with_iterable_data = AsciiWriter.write(TestDataset.function_of_scalar_with_iterable_data)
  //def function_of_tuple_with_data_from_kids = AsciiWriter.write(TestDataset.function_of_tuple_with_data_from_kids)
  //def function_of_tuple_with_iterable_data = AsciiWriter.write(TestDataset.function_of_tuple_with_iterable_data)
  def function_of_tuple_with_mixed_types = AsciiWriter.write(TestDataset.function_of_tuple_with_mixed_types)
}

object TestDataset {
  
  //implicit def stringToMetadata(name: String): Metadata = Metadata(name)
  
  def empty = Dataset(List(), Metadata("emptyDS"))
  
  def real = Dataset(Real(Metadata("myReal"), 3.14), Metadata("realDS"))
  
  
  def integer = Dataset(Integer(Metadata("myInteger"), 42), Metadata("intDS"))
  def text = Dataset(Text(Metadata("myText"), "Hi"), Metadata("textDS"))
  def time = Dataset(Time(Metadata("myRealTime"), 1000.0), Metadata("timeDS"))
  //TODO: int_time, text_time
  def scalars = Dataset(List(Real(Metadata("myReal"), 3.14), Integer(Metadata("myInteger"), 42), Text(Metadata("myText"), "Hi"), Time(Metadata("myRealTime"), 1000.0)), Metadata("scalarDS"))
  
//  def tuple_of_scalars
//  def tuple_of_tuple_text
//  def tuples
//  def scalar_tuple
//  def tuple_scalar
//  

  def function_of_scalar_with_data_from_kids = {
    Dataset(Function.fromValues(List(List(0.0, 1.0, 2.0), List(0.0, 1.0, 2.0))))
  }

  def function_of_scalar_with_iterable_data = {
    val samples = List(Sample(Real(0), Real(0)), 
                       Sample(Real(1), Real(1)), 
                       Sample(Real(2), Real(2)))
    Dataset(Function(samples))
  }

  def function_of_tuple_with_data_from_kids = {
    Dataset(Function.fromValues(List(List(0.0, 1.0, 2.0), List(0.0, 1.0, 2.0), List(10.0, 11.0, 12.0))))
  }

  def function_of_tuple_with_iterable_data = {
    val samples = List(Sample(Real(0), Tuple(Real(0), Real(10))), 
                       Sample(Real(1), Tuple(Real(1), Real(11))), 
                       Sample(Real(2), Tuple(Real(2), Real(12))))
    Dataset(Function(samples))
  }
  
  def function_of_tuple_with_mixed_types = {
    val samples = List(Sample(Integer(0), Tuple(Real(0), Text("zero"))), 
                       Sample(Integer(1), Tuple(Real(1), Text("one"))), 
                       Sample(Integer(2), Tuple(Real(2), Text("two"))))
    Dataset(Function(samples))
  }
  
//  def function_of_function  See TestNestedFunction
//  def time_series_of_scalar
//  def time_series_of_tuple
  
  def empty_function = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty))
  
  //def index_function = Dataset(Function(List(Text("msg", "Hi"), Text("msg", "Bye")), "myIndexFunction"), "indexFunctionDS")
  def index_function = Dataset(Function(List(Integer(1), Integer(2)), Metadata("myIndexFunction")), Metadata("indexFunctionDS"))
  
  //TODO: time_series with res: micro, milli, sec, hour, day, year
  
}