package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.metadata.Metadata

class TestProjection {
  //TODO: use TestDataset-s
  
  //implicit def stringToMetadata(name: String): Metadata = Metadata(name)
  
  @Test
  def project_scalar_include {
    val r = Real(Metadata("a"))
    val proj = new Projection(List("a","c"))
    val ds = proj(r)
    assertEquals(1, ds.getLength)
  }
  
  @Test
  def project_scalar_alt_form {
    val ds = Dataset(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val ds2 = ds.project(Projection(List("b")))
    assertEquals("b", ds2(0).getName)
  }
  
  @Test
  def project_scalar_alt_form_with_names {
    val ds = Dataset(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val ds2 = ds.project(List("b"))
    assertEquals("b", ds2(0).getName)
  }
  
  @Test
  def project_scalar_exclude {
    val r = Real(Metadata("a"))
    val proj = new Projection(List("b","c"))
    val ds = proj(r)
    assertEquals(0, ds.getLength)
  }
  
  @Test
  def project_scalar_in_tuple {
    val tup = Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val proj = new Projection(List("b"))
    val ds = proj(tup)
    val n = ds(0).asInstanceOf[Tuple].getLength
    assertEquals(1, n)
  }
  
  @Test
  def project_scalars_in_tuple {
    val tup = Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val proj = new Projection(List("a","c"))
    val ds = proj(tup)
    val n = ds(0).asInstanceOf[Tuple].getLength
    assertEquals(2, n)
  }
  
  @Test
  def project_scalar_in_function_scalar_range {
    val f = Function(Real(Metadata("t")), Real(Metadata("a")))
    val proj = new Projection(List("t","a"))
    val ds = proj(f)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds(0).asInstanceOf[Function].getRange.toSeq.length
    assertEquals(1, n)
  }
  
  @Test
  def project_scalar_in_function_tuple_range {
    val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val proj = new Projection(List("t","b"))
    val ds = proj(f)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds(0).asInstanceOf[Function].getRange.asInstanceOf[Tuple].getLength
    assertEquals(1, n)
  }
  
  @Test
  def project_scalars_in_function_tuple_range {
    val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val proj = new Projection(List("t","b","a")) //note, diff order, but not used, //TODO: enforce order
    val ds = proj(f)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds(0).asInstanceOf[Function].getRange.asInstanceOf[Tuple].getLength
    assertEquals(2, n)
  }
  
  @Test
  def project_scalars_in_function_function_range {
    val f = Function(Real(Metadata("t")), Function(Real(Metadata("w")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))))
    val proj = new Projection(List("t","w","b","c")) 
    val ds = proj(f)
    val n = ds(0).asInstanceOf[Function].getRange.asInstanceOf[Function].getRange.asInstanceOf[Tuple].getLength
    assertEquals(2, n)
    //println(ds)
  }
  
  //TODO: project nothing
  //TODO: def project_scalar_in_function_scalar_range_without_domain => IndexFunction
  //TODO: domain only
  //TODO: reorder
  
  //def project_named_tuple
  //def project_named_function
  //TODO: nested use cases
  
  //TODO: project same var twice
  
  
  @Test def construct_from_expression_of_one = assertEquals(1, Projection("a").names.length)
  @Test def construct_from_expression_of_two = assertEquals(2, Projection("a,b").names.length)
  @Test def construct_from_expression_of_three = assertEquals(3, Projection("a,b,c").names.length)
  @Test def construct_from_expression_with_spaces = assertEquals(2, Projection("a,  b").names.length)
  @Test def construct_from_expression_with_space_padding = assertEquals(2, Projection(" a,b ").names.length)
}




