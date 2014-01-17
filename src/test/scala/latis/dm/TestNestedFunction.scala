package latis.dm

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter
import latis.writer.Writer

class TestNestedFunction {

  //@Test
  def function_of_functions {
    val n = 4
    val domain  = (0 until n).map(Integer(_))
    val range = for (i <- 0 until n) yield Function((0 until 3).map(j => Sample(Integer(10 + j), Real(10 * i + j))))
    val ds = Dataset(Function(domain, range))
    AsciiWriter.write(ds)
    //Writer("json").write(ds)
  }
  
  @Test
  def tuple_of_functions {
    val fs = for (i <- 0 until 4) yield Function((0 until 3).map(j => Sample(Integer(10 + j), Real(10 * i + j))))
    val ds = Dataset(Tuple(fs))
    AsciiWriter.write(ds)
  }
}