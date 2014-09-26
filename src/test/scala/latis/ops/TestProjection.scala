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
    assertEquals(1, ds.getElementCount)
  }
  
  @Test
  def project_scalar_alt_form {
    val ds = Dataset(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val ds2 = ds.project(Projection(List("b")))
    assertEquals("b", ds2.getVariables(0).getName)
  }
  
  @Test
  def project_scalar_alt_form_with_names {
    val ds = Dataset(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val ds2 = ds.project(List("b"))
    assertEquals("b", ds2.getVariables(0).getName)
  }
  
  @Test
  def project_scalar_exclude {
    val r = Real(Metadata("a"))
    val proj = new Projection(List("b","c"))
    val ds = proj(r)
    assertEquals(0, ds.getElementCount)
  }
  
  @Test
  def project_scalar_in_tuple {
    val tup = Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val proj = new Projection(List("b"))
    val ds = proj(tup)
    val n = ds.getVariables(0).asInstanceOf[Tuple].getElementCount
    assertEquals(1, n)
  }
  
  @Test
  def project_scalars_in_tuple {
    val tup = Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val proj = new Projection(List("a","c"))
    val ds = proj(tup)
    val n = ds.getVariables(0).asInstanceOf[Tuple].getElementCount
    assertEquals(2, n)
  }
  
  @Test
  def project_scalar_in_function_scalar_range {
    //val f = Function(Real(Metadata("t")), Real(Metadata("a")))
    val ds1 = TestDataset.function_of_named_scalar
    val proj = new Projection(List("t","a"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds2.getVariables(0).asInstanceOf[Function].getRange.toSeq.length
    assertEquals(1, n)
  }
  
  @Test
  def project_scalar_in_function_tuple_range {
    //val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val ds1 = TestDataset.function_of_tuple
    val proj = new Projection(List("myInteger","myReal"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds2.getVariables(0).asInstanceOf[Function].getRange.asInstanceOf[Tuple].getElementCount
    assertEquals(1, n)
  }
  
  @Test
  def project_scalars_in_function_tuple_range {
    //val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val ds1 = TestDataset.function_of_tuple
    val proj = new Projection(List("myInteger","myReal", "myText"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds2.getVariables(0).asInstanceOf[Function].getRange.asInstanceOf[Tuple].getElementCount
    assertEquals(2, n)
  }
  
  @Test
  def project_scalar_in_function_function_range {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t","w","a")) 
    val ds2 = proj(ds1)
    val n = ds2.getVariables(0).asInstanceOf[Function].getRange.asInstanceOf[Function].getRange.asInstanceOf[Tuple].getElementCount
    assertEquals(1, n)
  }
  
  @Test
  def project_all_but_outer_domain_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("w","a","b")) 
    val ds2 = proj(ds1)
    //TODO: ds1 appears empty, iterable once problem? need to rewind byte buffers?  AsciiWriter.write(ds1)
    val f = ds2.getVariables(0).asInstanceOf[Function]
    val domain = f.getDomain
    assertEquals("index", domain.getName)
    
    val n = ds2.getLength
    assertEquals(4, n)
  }
  
  //@Test
  def project_all_but_inner_domain_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t","a","b")) 
    //AsciiWriter.write(ds1)
    val ds2 = proj(ds1)
//TODO: range of first sample disappears from ds1 after projection!? 
    AsciiWriter.write(ds1)
  }
  
  @Test
  def project_outer_domain_only_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t")) 
    val ds2 = proj(ds1)
    val f = ds2.getVariables(0).asInstanceOf[Function]
    val domain = f.getDomain
    val range  = f.getRange
    assertEquals("index", domain.getName)
    assertEquals("t", range.getName)
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




