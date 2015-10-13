package latis.dm

import org.junit.Test
import org.junit.Assert._
import latis.metadata.Metadata

class TestVariable {
  
  @Test
  def findVariable {
    val v = Tuple(Seq(
        Tuple(Real(Metadata("a")), Metadata("bar")), 
        Tuple(Real(Metadata("b")), Metadata("bar"))
        ), Metadata("foo"))
    
    v.findVariableByName("a") match {
      case None => fail
      case Some(_) => 
    }
  }
  
  @Test
  def findVariable_with_long_name {
    val v = Tuple(Seq(
        Tuple(Real(Metadata("a")), Metadata("bar")), 
        Tuple(Real(Metadata("b")), Metadata("bar"))
        ), Metadata("foo"))
    
    v.findVariableByName("foo.bar.a") match {
      case None => fail
      case Some(_) => 
    }
    v.findVariableByName("foo.bar.b") match {
      case None => fail
      case Some(_) => 
    }
  }
  
  @Test 
  def findAllVariables {
    val v = Tuple(Seq(
        Tuple(Real(Metadata("a")), Metadata("bar")), 
        Tuple(Real(Metadata("b")), Metadata("bar"))
        ), Metadata("foo"))
    
    v.findAllVariablesByName("foo.bar") match {
      case Seq(v1, v2) => 
      case _ => fail
    }
    v.findAllVariablesByName("foo.bar.b") match {
      case Seq(v1) => 
      case _ => fail
    }
  }
  
}