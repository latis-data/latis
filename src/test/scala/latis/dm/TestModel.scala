package latis.dm

import org.junit._
import org.junit.Assert._
import latis.metadata.Metadata


class TestModel {
  
  @Test
  def model = {
    val a = ScalarType("A")
    val b = ScalarType("B")
    val x = ScalarType("X")
    val v = FunctionType(x, TupleType(Seq(a,b)))
    val m = Model(v)
    println(m)
  }
}