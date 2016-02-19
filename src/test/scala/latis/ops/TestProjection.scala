package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.TestDataset
import latis.dm.TestNestedFunction
import latis.dm.Tuple
import latis.dm.implicits.variableToDataset
import latis.metadata.Metadata
import latis.ops.filter.FirstFilter
import latis.ops.filter.Selection
import latis.dm.Index
import latis.dm.Sample

class TestProjection {
  //TODO: use TestDataset-s
  //TODO: should projection of a non-existing variable be an error? consider sql, relational algebra
  
  //implicit def stringToMetadata(name: String): Metadata = Metadata(name)
  
//  @Test
//  def project_scalar_include {
//    val r = Real(Metadata("a"))
//    val proj = new Projection(List("a","c"))
//    val ds = proj(r)
//    assertEquals(1, ds.getElementCount)
//  }
  
  @Test
  def project_scalar_alt_form {
    val ds = Dataset(Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val ds2 = ds.project(Projection(List("b")))
    assertEquals("b", ds2.unwrap.asInstanceOf[Tuple].getVariables(0).getName)
  }
  
//  @Test
//  def project_scalar_alt_form_with_names {
//    val ds = Dataset(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
//    val ds2 = ds.project(List("b"))
//    assertEquals("b", ds2.getVariables(0).getName)
//  }
  
  @Test
  def project_scalar_exclude {
    val r = Real(Metadata("a"))
    val proj = new Projection(List("b","c"))
    val ds = proj(r)
    assert(ds.isEmpty)
  }
  
  @Test
  def project_scalar_in_tuple {
    val tup = Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val proj = new Projection(List("b"))
    val ds = proj(tup)
    val n = ds.unwrap.asInstanceOf[Tuple].getElementCount
    assertEquals(1, n)
  }
  
  @Test
  def project_scalars_in_tuple {
    val tup = Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val proj = new Projection(List("a","c"))
    val ds = proj(tup)
    val n = ds.unwrap.asInstanceOf[Tuple].getElementCount
    assertEquals(2, n)
  }
  
  @Test
  def project_scalar_in_function_scalar_range {
    //val f = Function(Real(Metadata("t")), Real(Metadata("a")))
    val ds1 = TestDataset.function_of_named_scalar
    val proj = new Projection(List("t","a"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds2.unwrap.asInstanceOf[Function].getRange.toSeq.length
    assertEquals(1, n)
  }
  
  @Test
  def project_scalar_in_function_tuple_range {
    //val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val ds1 = TestDataset.function_of_tuple
    val proj = new Projection(List("myInteger","myReal"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds2.unwrap.asInstanceOf[Function].getRange.asInstanceOf[Tuple].getElementCount
    assertEquals(1, n)
  }
  
  @Test
  def project_scalars_in_function_tuple_range {
    //val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val ds1 = TestDataset.function_of_tuple
    val proj = new Projection(List("myInteger","myReal", "myText"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    val n = ds2.unwrap.asInstanceOf[Function].getRange.asInstanceOf[Tuple].getElementCount
    assertEquals(2, n)
  }
  
  @Test
  def project_scalar_in_function_function_range {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t","w","a")) 
    val ds2 = proj(ds1)
    val n = ds2.unwrap.asInstanceOf[Function].getRange.asInstanceOf[Function].getRange.asInstanceOf[Tuple].getElementCount
    assertEquals(1, n)
  }
  
  @Test
  def project_all_but_outer_domain_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("w","a","b")) 
    val ds2 = proj(ds1)
    val f = ds2.unwrap.asInstanceOf[Function]
    val domain = f.getDomain
    assertEquals("index", domain.getName)
    
    val n = ds2.getLength
    assertEquals(4, n)
  }
  
  @Test
  def project_all_but_inner_domain_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t","a","b")) 
    val ds2 = proj(ds1)
    val f = ds2.unwrap.asInstanceOf[Function]
    val domain = f.getDomain
    val range  = f.getRange.asInstanceOf[Function]
    assertEquals("t", domain.getName)
    assertEquals(4, ds2.getLength)
    assertEquals(3, range.getLength)
  }
  
  @Test
  def project_outer_domain_only_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t")) 
    val ds2 = proj(ds1)
    val f = ds2.unwrap.asInstanceOf[Function]
    val domain = f.getDomain
    val range  = f.getRange
    assertEquals("index", domain.getName)
    assertEquals("t", range.getName)
  }
  
  @Test
  def project_inner_domain_only_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("w")) 
    val ds2 = proj(ds1)
    val f = ds2.unwrap.asInstanceOf[Function]
    val domain = f.getDomain
    val range  = f.getRange.asInstanceOf[Function]
    assertEquals("index", domain.getName)
    assertEquals("w", range.getRange.getName)
    assertEquals(3, range.getLength)
  }
  
  @Test
  def project_domains_only_in_function_function {
    //make sure domain of nested function becomes range when range not projected
    // (t -> index -> w)
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t","w")) 
    val ds2 = proj(ds1)
    ds2 match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Function(it2)) => it2.next match {
          case Sample(_: Index, w) => assertEquals("w", w.getName)
        }
      }
    }
  }
  
  @Test
  def get_first_wavelength_set_in_function_function {
    val ds1 = FirstFilter()(Dataset(TestNestedFunction.function_of_functions_with_tuple_range))
    val proj = new Projection(List("w")) 
    val ds2 = proj(ds1)
    val f = ds2.unwrap.asInstanceOf[Function]
    val range  = f.getRange.asInstanceOf[Function]
    assertEquals(1, f.getLength)
    assertEquals(3, range.getLength)
  }
  
  @Test
  def select_on_w_then_project_others {
    val ds1 = Selection("w=12")(Dataset(TestNestedFunction.function_of_functions_with_tuple_range))
    val proj = new Projection(List("t","b")) 
    val ds2 = proj(ds1)
    val f = ds2.unwrap.asInstanceOf[Function]
    val range  = f.getRange.asInstanceOf[Function]
    assertEquals(4, f.getLength)
    //assertEquals(1, range.getLength) //Selection does not update Metadata
    //the data is right when I use a writer, but I can't access it here correctly
    //assertEquals(2, range.getRange.toSeq(0).getNumberData.longValue) 
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




