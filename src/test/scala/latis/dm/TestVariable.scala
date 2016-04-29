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
    //foo: (bar: (a), bar: (b))
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
  
  @Test
  def find_all_variables_by_middle_name = {
    //foo: (bar: (a, b))
    val v = Tuple(Seq(
        Tuple(Seq(Real(Metadata("a")), Real(Metadata("b"))), Metadata("bar"))
        ), Metadata("foo"))
    
    v.findAllVariablesByName("bar") match {
      case Seq(Tuple(vars)) => assertEquals(2, vars.length)
      case _ => fail
    }
    v.findAllVariablesByName("bar.b") match {
      case Seq(v1: Real) => assertEquals("b", v1.getName)
      case _ => fail
    }
  }
  
  @Test
  def find_variable_without_middle_name = {
    //foo: (bar: (a, b))
    val v = Tuple(Seq(
        Tuple(Seq(Real(Metadata("a")), Real(Metadata("b"))), Metadata("bar"))
        ), Metadata("foo"))
        
    v.findVariableByName("foo.a") match {
      case None => fail
      case Some(v1: Variable) => assertEquals("a", v1.getName)
    }
  }
}