package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter

/*
 * TODO: provide a Seq of Datasets that can be fed to various tests such as getLength
 */

class TestDataset {
  
  //@Test
  //def function_of_scalar_with_data_from_kids = AsciiWriter.write(TestDataset.function_of_scalar_with_data_from_kids)
  //def function_of_scalar_with_iterable_data = AsciiWriter.write(TestDataset.function_of_scalar_with_iterable_data)
  //def function_of_tuple_with_data_from_kids = AsciiWriter.write(TestDataset.function_of_tuple_with_data_from_kids)
  //def function_of_tuple_with_iterable_data = AsciiWriter.write(TestDataset.function_of_tuple_with_iterable_data)
  def function_of_tuple_with_mixed_types = AsciiWriter.write(TestDataset.function_of_tuple_with_mixed_types)
}

/**
 * Static datasets to use for testing.
 */
object TestDataset {
  //TODO: delegate to Variable specific tests, wrap in Dataset as needed
  //TODO: consider a single collection of Datasets that can be used by anything that needs to be tested with datasets, by extending trait?
  
  //implicit def stringToMetadata(name: String): Metadata = Metadata(name)
  
  def empty = Dataset(List(), Metadata("emptyDS"))
  
  def real = Dataset(Real(Metadata("myReal"), 3.14), Metadata("realDS"))
  def integer = Dataset(Integer(Metadata("myInteger"), 42), Metadata("intDS"))
  def text = Dataset(Text(Metadata("myText"), "Hi"), Metadata("textDS"))
  def real_time = Dataset(Time(Metadata("myRealTime"), 1000.0), Metadata("timeDS"))
  def text_time = Dataset(Time(Metadata(Map[String, String]("name" -> "myTextTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")), "1970/01/01"), Metadata("text_timeDS"))
  def int_time = Dataset(Time(Metadata(Map[String, String]("name" -> "myIntegerTime", "type" -> "integer")), 1000), Metadata("integer_timeDS"))
  def scalars = Dataset(List(Real(Metadata("myReal"), 3.14), Integer(Metadata("myInteger"), 42), Text(Metadata("myText"), "Hi"), Time(Metadata("myRealTime"), 1000.0)), Metadata("scalarDS"))
  
  def tuple_of_scalars = Dataset(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero")), Metadata("tupleDS"))
  def tuple_of_tuples = Dataset(Tuple(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), Tuple(Integer(Metadata("myInteger"), 1), Real(Metadata("myReal"), 1.1))), Metadata("tuple_of_tuplesDS"))
  def tuple_of_functions = Dataset(TestNestedFunction.tuple_of_functions)
  def scalar_tuple = Dataset(Tuple(Integer(Metadata("myInteger"), 1)))
  def mixed_tuple = Dataset(Tuple(Real(Metadata("myReal"), 0.0), Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), function_of_scalar_with_iterable_data.getVariables(0)), Metadata("mixed_tuple"))
//  def tuple_of_tuple_text
//  def tuple_scalar
//  

//  def function_of_scalar_with_data_from_kids = {
//    Dataset(Function.fromValues(List(List(0.0, 1.0, 2.0), List(0.0, 1.0, 2.0))))
//  }

  def function_of_scalar_with_iterable_data = {
    val samples = List(Sample(Real(0), Real(0)), 
                       Sample(Real(1), Real(1)), 
                       Sample(Real(2), Real(2)))
    Dataset(Function(samples))
  }

//  def function_of_tuple_with_data_from_kids = {
//    Dataset(Function.fromValues(List(List(0.0, 1.0, 2.0), List(0.0, 1.0, 2.0), List(10.0, 11.0, 12.0))))
//  }

  def function_of_tuple_with_iterable_data = {
    val samples = List(Sample(Real(0), Tuple(Real(0), Real(10))), 
                       Sample(Real(1), Tuple(Real(1), Real(11))), 
                       Sample(Real(2), Tuple(Real(2), Real(12))))
    Dataset(Function(samples))
  }
  
  def function_of_tuple_with_mixed_types = {
    val samples = List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero"))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 1), Text(Metadata("myText"), "one"))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two"))))
    Dataset(Function(samples))
  }
  
  def function_of_functions = Dataset(TestNestedFunction.function_of_functions_with_sampled_data)
  
  def mixed_function = {
    val samples = List(Sample(Real(Metadata("myReal"), 0.0), Tuple(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), (function_of_scalar_with_iterable_data+(0)).getVariables(0))),
                       Sample(Real(Metadata("myReal"), 1.1), Tuple(Tuple(Integer(Metadata("myInteger"), 1), Real(Metadata("myReal"), 1)), (function_of_scalar_with_iterable_data+(1)).getVariables(0))),
                       Sample(Real(Metadata("myReal"), 2.2), Tuple(Tuple(Integer(Metadata("myInteger"), 2), Real(Metadata("myReal"), 2)), (function_of_scalar_with_iterable_data+(2)).getVariables(0))))
    Dataset(Function(samples), Metadata("mixed_function"))
  }
  
  def time_series_of_scalar = {
    val samples = List(Sample(Time(Metadata("myTime"), 1000.0), Integer(Metadata("myInteger"), 3)),
                       Sample(Time(Metadata("myTime"), 2000.0), Integer(Metadata("myInteger"), 2)),
                       Sample(Time(Metadata("myTime"), 3000.0), Integer(Metadata("myInteger"), 1)))
    Dataset(Function(samples))
  }
  def time_series_of_tuple = {
    val md = Map[String, String]("name" -> "time", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val samples = List(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("int"), 1), Real(Metadata("real"), 1.1), Text(Metadata("text"), "A"))),
                       Sample(Time(Metadata(md), "1970/01/02"), Tuple(Integer(Metadata("int"), 2), Real(Metadata("real"), 2.2), Text(Metadata("text"), "B"))),
                       Sample(Time(Metadata(md), "1970/01/03"), Tuple(Integer(Metadata("int"), 3), Real(Metadata("real"), 3.3), Text(Metadata("text"), "C"))))
    Dataset(Function(samples))
    
  }
  
  def empty_function = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty))
  
  //def index_function = Dataset(Function(List(Text("msg", "Hi"), Text("msg", "Bye")), "myIndexFunction"), "indexFunctionDS")
  def index_function = Dataset(Function(List(Integer(1), Integer(2))), Metadata("indexFunctionDS"))
  
  //TODO: time_series with res: micro, milli, sec, hour, day, year
  
  def combo = Dataset(function_of_tuple_with_iterable_data.getVariables(0), tuple_of_tuples.getVariables(0), text.getVariables(0))
  
}