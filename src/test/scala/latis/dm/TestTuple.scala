package latis.dm

import latis.dm.implicits._
import latis.writer._

import org.junit._
import Assert._

class TestTuple {
 
  //@Test
  def write_tuple_of_real = AsciiWriter.write(TestTuple.tuple_of_real)
  
  //@Test
  def write_tuple_of_reals = AsciiWriter.write(TestTuple.tuple_of_reals)
  
  @Test
  def arity_one {
    assertEquals(1, TestTuple.tuple_of_real.getVariables.length)
  }  
  
  @Test
  def arity_two {
    assertEquals(2, TestTuple.tuple_of_reals.getVariables.length)
  }
}

object TestTuple {

  def tuple_of_real = Tuple(TestReal.pi)
  def tuple_of_reals = Tuple(TestReal.pi, TestReal.e)
  
  //TODO: named_tuple B:(x,y)
}