package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.ops.filter._

class ProjectionTest {
  
  @Test
  def project_scalar_include {
    val r = Real("a")
    val filter = new ProjectionFilter(List("a","c"))
    val ds = filter(r)
    assertEquals(1, ds.variables.length)
  }
  
  @Test
  def project_scalar_exclude {
    val r = Real("a")
    val filter = new ProjectionFilter(List("b","c"))
    val ds = filter(r)
    assertEquals(0, ds.variables.length)
  }
  
  @Test
  def project_scalar_in_tuple {
    val tup = Tuple(Real("a"), Real("b"), Real("c"))
    val filter = new ProjectionFilter(List("a","c"))
    val ds = filter(tup)
    val n = ds.getVariableByIndex(0).asInstanceOf[Tuple].variables.length
    assertEquals(2, n)
  }
  
  @Test
  def project_scalar_in_function_scalar_range {
    val f = Function(Real("t"), Real("a"))
    val filter = new ProjectionFilter(List("t","a"))
    val ds = filter(f)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds.getVariableByIndex(0).asInstanceOf[Function].range.toSeq.length
    assertEquals(1, n)
  }
  
  @Test
  def project_scalar_in_function_tuple_range {
    val f = Function(Real("t"), Tuple(Real("a"), Real("b"), Real("c")))
    val filter = new ProjectionFilter(List("t","b"))
    val ds = filter(f)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds.getVariableByIndex(0).asInstanceOf[Function].range.asInstanceOf[Tuple].variables.length
    assertEquals(1, n)
  }
  
  @Test
  def project_scalars_in_function_tuple_range {
    val f = Function(Real("t"), Tuple(Real("a"), Real("b"), Real("c")))
    val filter = new ProjectionFilter(List("t","b","a")) //note, diff order, but not used
    val ds = filter(f)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds.getVariableByIndex(0).asInstanceOf[Function].range.asInstanceOf[Tuple].variables.length
    assertEquals(2, n)
  }
  
  @Test
  def project_scalars_in_function_function_range {
    val f = Function(Real("t"), Function(Real("w"), Tuple(Real("a"), Real("b"), Real("c"))))
    val filter = new ProjectionFilter(List("t","w","b","c")) 
    val ds = filter(f)
    val n = ds.getVariableByIndex(0).asInstanceOf[Function].range.asInstanceOf[Function].range.asInstanceOf[Tuple].variables.length
    assertEquals(2, n)
    //println(ds)
  }
  
  //TODO: def project_scalar_in_function_scalar_range_without_domain => IndexFunction
  //TODO: reorder
  
  //def project_named_tuple
  //def project_named_function
  //TODO: nested use cases
  
  
}




