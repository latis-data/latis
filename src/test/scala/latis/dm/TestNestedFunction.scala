package latis.dm

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter
import latis.writer.Writer
import latis.metadata.Metadata

class TestNestedFunction {

  //@Test
  def function_of_functions {
    val ds = Dataset(TestNestedFunction.function_of_functions)
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
    
  def function_of_functions = {
    val n = 4
    val domain  = (0 until n).map(Integer(_))
    val range = for (i <- 0 until n) yield Function((0 until 3).map(j => Sample(Integer(10 + j), Real(10 * i + j))))
    Function(domain, range)
  }
    
  def tuple_of_functions = {
    val fs = for (i <- 0 until 4) yield Function((0 until 3).map(j =>
      Sample(Integer(Metadata("myInt"+i), 10 + j), Real(Metadata("myReal"+i), 10 * i + j))))
    Tuple(fs)
  }
  
  
}