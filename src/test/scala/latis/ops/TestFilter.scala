package latis.ops

import latis.dm._
import latis.writer.AsciiWriter
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import scala.collection._
import scala.collection.mutable.ArrayBuffer

class TestFilter {

  val dataset = Dataset(Function.fromValues(List(1.1,2.2,3.3), List(4.4,5.5,6.6)))
  
  @Test
  def test_last_filter {
    val ds = LastFilter().filter(dataset).toDoubles
    assertEquals(3.3, ds(0)(0), 0.0)
    assertEquals(6.6, ds(1)(0), 0.0)
  }
  
  @Test
  def test_first_filter {
    val ds = FirstFilter().filter(dataset).toDoubles
    assertEquals(1.1, ds(0)(0), 0.0)
    assertEquals(4.4, ds(1)(0), 0.0)
  }

}