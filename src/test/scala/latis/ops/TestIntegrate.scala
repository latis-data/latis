package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.TestDataset
import latis.dm.Tuple
import latis.metadata.Metadata

class TestIntegrate {
  
  @Test
  def test_canonical = {
    val expected = Real(3.456E8)
    assertEquals(expected, Integrate()(TestDataset.canonical).getVariables(0))
  }
  
  @Test
  def test_empty = {
    assertEquals(TestDataset.empty, Integrate()(TestDataset.empty))
  }
  
  @Test
  def test_scalar = {
    assertEquals(TestDataset.text, Integrate()(TestDataset.text))
  } 
  
  @Test
  def test_scalars = {
    assertEquals(TestDataset.scalars, Integrate()(TestDataset.scalars))
  }
  
  @Test
  def test_tuple_of_functions = {
    val ds = TestDataset.tuple_of_functions
    val expected = Tuple(Real(2), Real(22), Real(42), Real(62))
    assertEquals(expected, Integrate()(ds).getVariables(0))
  }
  
  @Test
  def test_function_of_scalars = {
    val ds = TestDataset.function_of_scalar
    val expected = Real(2)
    assertEquals(expected, Integrate()(ds).getVariables(0))
  }
  
  @Test
  def test_function_of_functions = {
    val ds = TestDataset.function_of_functions
    val expected = Sample(Integer(Metadata("x"), 0), Real(2))
    assertEquals(expected, Integrate()(ds).findFunction.get.iterator.next)
  }
  
  @Test
  def test_empty_function = {
    val ds = TestDataset.empty_function
    assertEquals(Real(0), Integrate()(ds).getVariables(0))
  }
  
}