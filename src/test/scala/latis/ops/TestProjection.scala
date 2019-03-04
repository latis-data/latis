package latis.ops

import org.junit.Assert._
import org.junit.Assert.fail
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
import scala.concurrent.Await

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
    ds2 match {
      case Dataset(v) => assertEquals("b", v.getName)
      case _ => fail()
    }
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
    ds match { case Dataset(v) => assert(v.isInstanceOf[Real]) }
  }
  
  @Test
  def project_scalars_in_tuple {
    val tup = Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c")))
    val proj = new Projection(List("a","c"))
    val ds = proj(tup)
    ds match {
      case Dataset(v) => assertEquals(2, v.asInstanceOf[Tuple].getElementCount)
      case _ => fail()
    }
  }
  
  @Test
  def project_scalar_in_function_scalar_range {
    //val f = Function(Real(Metadata("t")), Real(Metadata("a")))
    val ds1 = TestDataset.function_of_named_scalar
    val proj = new Projection(List("t","a"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    ds2 match {
      case Dataset(v) => assertEquals(1, v.asInstanceOf[Function].getRange.toSeq.length)
      case _ => fail()
    }
  }
  
  @Test
  def project_scalar_in_function_tuple_range {
    //val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val ds1 = TestDataset.function_of_tuple
    val proj = new Projection(List("myInteger","myReal"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    ds2 match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Real(v)) => assertEquals(0, v, 0.0)
      }
    }
  }
  
  @Test
  def project_scalars_in_function_tuple_range {
    //val f = Function(Real(Metadata("t")), Tuple(Real(Metadata("a")), Real(Metadata("b")), Real(Metadata("c"))))
    val ds1 = TestDataset.function_of_tuple
    val proj = new Projection(List("myInteger","myReal", "myText"))
    val ds2 = proj(ds1)
    //TODO: test same domain: val domain = ds.getVariableByIndex(0).asInstanceOf[Function].domain
    ds2 match {
      case Dataset(v) => assertEquals(2, v.asInstanceOf[Function].getRange.asInstanceOf[Tuple].getElementCount)
      case _ => fail()
    }
  }
  
  @Test
  def project_scalar_in_function_function_range {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t","w","a")) 
    val ds2 = proj(ds1)
    ds2 match {
      case Dataset(v) => assertTrue(v.asInstanceOf[Function].getRange.asInstanceOf[Function].getRange.isInstanceOf[Real])
      case _ => fail()
    }
  }
  
  @Test
  def project_all_but_outer_domain_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("w","a","b")) 
    val ds2 = proj(ds1)
    ds2 match {
      case Dataset(v) => {
        assertEquals("index", v.asInstanceOf[Function].getDomain.getName)
        assertEquals(4, ds2.getLength)
      }
      case _ => fail()
    }
  }
  
  @Test
  def project_all_but_inner_domain_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t","a","b")) 
    val ds2 = proj(ds1)
    ds2 match {
      case Dataset(v) => {
        assertEquals("t", v.asInstanceOf[Function].getDomain.getName)
        assertEquals(4, ds2.getLength)
        assertEquals(3, v.asInstanceOf[Function].getRange.asInstanceOf[Function].getLength)
      }
      case _ => fail()
    }
  }
  
  @Test
  def project_outer_domain_only_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("t")) 
    val ds2 = proj(ds1)
    ds2 match {
      case Dataset(v) => {
        assertEquals("index", v.asInstanceOf[Function].getDomain.getName)
        assertEquals("t", v.asInstanceOf[Function].getRange.getName)
      }
      case _ => fail()
    }
  }
  
  @Test
  def project_inner_domain_only_in_function_function {
    val ds1 = Dataset(TestNestedFunction.function_of_functions_with_tuple_range)
    val proj = new Projection(List("w")) 
    val ds2 = proj(ds1)
    ds2 match {
      case Dataset(v) => {
        assertEquals("index", v.asInstanceOf[Function].getDomain.getName)
        assertEquals("w", v.asInstanceOf[Function].getRange.asInstanceOf[Function].getRange.getName)
        assertEquals(3, v.asInstanceOf[Function].getRange.asInstanceOf[Function].getLength)
      }
      case _ => fail()
    }
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
    ds2 match {
      case Dataset(v) => {
        assertEquals(1, v.asInstanceOf[Function].getLength)
        assertEquals(3, v.asInstanceOf[Function].getRange.asInstanceOf[Function].getLength)
      }
      case _ => fail()
    }
  }
  
  @Test
  def select_on_w_then_project_others {
    val ds1 = Selection("w=12")(Dataset(TestNestedFunction.function_of_functions_with_tuple_range))
    val proj = new Projection(List("t","b")) 
    val ds2 = proj(ds1)
    //val range  = f.getRange.asInstanceOf[Function]
    ds2 match {
      case Dataset(v) => assertEquals(4, v.asInstanceOf[Function].getLength)
      case _ => fail()
    }
    //assertEquals(1, range.getLength) //Selection does not update Metadata
    //the data is right when I use a writer, but I can't access it here correctly
    //assertEquals(2, range.getRange.toSeq(0).getNumberData.longValue) 
  }
  

  @Test
  def project_nothing_in_scalar = {
    val ds = Dataset(Real(Metadata("x"), 3.14)).project("z")
    assert(ds.isEmpty)
  }
  
  @Test
  def project_nothing_in_tuple = {
    val ds = Dataset(Tuple(Real(Metadata("x"), 1), Real(Metadata("y"), 2))).project("z")
    assert(ds.isEmpty)
  }
  
  @Test
  def project_nothing_in_function = {
    val samples = List(Sample(Real(Metadata("x"), 1), Real(Metadata("y"), 2)))
    val ds = Dataset(Function(samples)).project("z")
    assert(ds.isEmpty)
  }
  
  @Test
  def project_nothing_in_index_function = {
    val samples = List(Sample(Index(), Real(Metadata("y"), 2)))
    val ds = Dataset(Function(samples)).project("z")
    assert(ds.isEmpty)
  }
  
  @Test
  def project_alias = {
    val r = Real(Metadata("a") + ("alias" -> "A"))
    val proj = new Projection(List("A"))
    val z = proj.applyToVariable(r)
    assert(z.nonEmpty)
  }
  
  
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




