package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time

object TestDataset {
  
  implicit def stringToMetadata(name: String): Metadata = Metadata(name)
  
  def empty = Dataset(List(), "emptyDS")
  
  def real = Dataset(Real("myReal", 3.14), "realDS")
  def integer = Dataset(Integer("myInteger", 42), "intDS")
  def text = Dataset(Text("myText", "Hi"), "textDS")
  def time = Dataset(Time("myRealTime", 1000.0), "timeDS")
  //TODO: int_time, text_time
  def scalars = Dataset(List(Real("myReal", 3.14), Integer("myInteger", 42), Text("myText", "Hi"), Time("myRealTime", 1000.0)), "scalarDS")
  
//  def tuple_of_scalars
//  def tuple_of_tuple_text
//  def tuples
//  def scalar_tuple
//  def tuple_scalar
//  
//  def function_of_scalar
//  def function_of_tuple
//  def function_of_function
//  def time_series_of_scalar
//  def time_series_of_tuple
  
  def empty_function = Dataset(Function(Real("domain"), Real("range"), Iterator.empty))
  
  def index_function = Dataset(Function(List(Text("msg", "Hi"), Text("msg", "Bye")), "myIndexFunction"), "indexFunctionDS")
  
  //TODO: time_series with res: micro, milli, sec, hour, day, year
  
}