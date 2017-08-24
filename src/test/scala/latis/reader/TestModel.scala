package latis.reader

import latis.dm._
import latis.data._

import org.junit._
import Assert._
import latis.reader.adapter.Adapter
import latis.data.value.DoubleValue
import latis.data.seq.DataSeq
import latis.metadata.Metadata
import latis.ops.Operation
import latis.reader.tsml.TsmlReader2
import java.net.URL
import latis.ops.filter.FirstFilter


class TestModel {
  
  val model = {
    val a = ScalarType("a", Metadata("type" -> "real"))
    val b = ScalarType("b", Metadata("type" -> "real"))
    val c = ScalarType("c", Metadata("type" -> "real"))
    val f = FunctionType(a,b, "f", Metadata("length" -> "10"))
    val g = FunctionType(c, f, "g", Metadata("length" -> "1"))
    Model(g)
  }
  
  @Test
  def to_string = {
    assertEquals("Model(c -> a -> b)", model.toString)
  }
  
  //@Test
  def scalars = {
    println(model.getScalars)
  }
  
  @Test
  def find = {
    model.findVariableByName("b") match {
      case Some(s: ScalarType) => assertEquals("b", s.getId)
      case _ => fail
    }
  }
  
  @Test
  def from_seq = {
    val a = ScalarType("a", Metadata("type" -> "real"))
    val b = ScalarType("b", Metadata("type" -> "real"))
    val f = FunctionType(a,b, "f", Metadata("length" -> "10"))
    val c = ScalarType("c", Metadata("type" -> "real"))
    val vts = List(f,c,b)
    val m = Model.fromSeq(vts)
    assertEquals("Model(c -> b)", m.toString)
  }
  
  @Test
  def map = {
    val m = model.map { v => 
      if (v.hasName("a")) ScalarType("d", Metadata("type" -> "real"))
      else v
    }
    assertEquals("Model(c -> d -> b)", m.toString)
  }
  
  //@Test
  def order = {
    //model.foreach(println(_))
  }

}





