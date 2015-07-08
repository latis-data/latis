package latis.ops

import org.junit.Assert._
import org.junit.Test
import latis.dm.Integer
import latis.dm.Function
import latis.dm.Number
import latis.dm.Real
import latis.dm.Sample
import latis.dm.TestDataset
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.writer.AsciiWriter
import latis.dm.Dataset
import org.junit.Ignore

class TestIntegrate {
  
  @Test
  def test_canonical = {
    val expected = Real(3.456E8)
    assertEquals(expected, RiemannTrapezoidIntegration()(TestDataset.canonical).unwrap)
  }
  
  //TODO: error?
  @Test @Ignore
  def test_empty = {
    assertEquals(Dataset.empty, RiemannTrapezoidIntegration()(Dataset.empty))
  }
  
  //TODO: integrate Text, error?
  @Test @Ignore
  def test_scalar = {
    assertEquals(TestDataset.text, RiemannTrapezoidIntegration()(TestDataset.text))
  } 
  
//  @Test
//  def test_scalars = {
//    assertEquals(TestDataset.scalars, RiemannTrapezoidIntegration()(TestDataset.scalars))
//  }
  
  @Test
  def test_tuple_of_functions = {
    val ds = TestDataset.tuple_of_functions
    val ds2 = RiemannTrapezoidIntegration()(ds)
    
    ds2 match {
      case Dataset(tup: Tuple) => {
        assertEquals(2, tup.getVariables(0).getNumberData.doubleValue, 0.0)
        assertEquals(22, tup.getVariables(1).getNumberData.doubleValue, 0.0)
        assertEquals(42, tup.getVariables(2).getNumberData.doubleValue, 0.0)
        assertEquals(62, tup.getVariables(3).getNumberData.doubleValue, 0.0)
      }
      case _ => fail
    }
  }
  
  @Test
  def test_function_of_scalars = {
    val ds = TestDataset.function_of_scalar
    val expected = Real(2)
    assertEquals(expected, RiemannTrapezoidIntegration()(ds).unwrap)
  }
  
  @Test
  def test_function_of_functions = {
    val ds = TestDataset.function_of_functions
    val expected = Sample(Integer(Metadata("x"), 0), Real(2))
    assertEquals(expected, RiemannTrapezoidIntegration()(ds).unwrap.asInstanceOf[Function].iterator.next)
  }
  
  @Test
  def test_empty_function = {
    val ds = TestDataset.empty_function
    assertEquals(Real(0), RiemannTrapezoidIntegration()(ds).unwrap)
  }
  
  @Test
  def domain_spacing_of_2 {
    val samples = List(Sample(Real(0), Real(1)),
                       Sample(Real(2), Real(2)),
                       Sample(Real(4), Real(3)))
    val ds = Dataset(Function(samples))
    val ds2 = RiemannTrapezoidIntegration()(ds)
    ds2.unwrap match {
      case Number(n) => assertEquals(8.0, n, 0.0)
      case _ => fail
    }
  } 
  
  @Test
  def sample_spectrum {
    //http://localhost:8080/lisird3/latis/sorce_ssi_l3.asc?&time~2010-01-02T12&wavelength>1&wavelength<4
    val samples = List(Sample(Real(1.5), Real(2.6310237217330723E-5)),
                       Sample(Real(2.5), Real(1.686556197455502E-5)),
                       Sample(Real(3.5), Real(8.610772692918544E-6)))
    val ds = Dataset(Function(samples))
    val ds2 = NewtonCotesIntegration()(ds)
    ds2.unwrap match {
      case Number(n) => assertEquals(3.41806e-05, n, 1e-8)
      //IDL's int_tabulated (5-point Newton-Cotes) produces 3.41806e-05
      //Ours produces 3.4326066929679656E-5
      case _ => fail
    }
  }

}