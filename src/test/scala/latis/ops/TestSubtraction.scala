package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.Sample
import latis.dm.TestDataset
import latis.dm.Tuple
import latis.dm.implicits.doubleToDataset

class TestSubtraction {
  
  @Test
  def test_empty {
    assertEquals(Dataset.empty, Dataset.empty - 899)
  }
  @Test
  def test_real_value {
    assertEquals(0, (3.14 - TestDataset.real).unwrap.getNumberData.doubleValue, 0.0)
  }
  @Test
  def test_real_md {
    assertEquals("realDS", (TestDataset.real - 3.14).getName)
  }
  @Test
  def test_integer_value {
    assertEquals(-44, ((-2) - TestDataset.integer).unwrap.getNumberData.intValue)
  }
  @Test
  def test_integer_md {
    assertEquals("intDS", (TestDataset.integer - (-2)).getName)
  }
  @Test
  def test_text {
    assertEquals(Double.NaN, (TestDataset.text - 1).unwrap.getNumberData.doubleValue, 0.0)
  }
  @Test
  def test_time {
    assertEquals(500, (TestDataset.real_time - 500).unwrap.getNumberData.doubleValue, 0.0)
  }
  @Test
  def test_tuple_of_tuples {
    assertEquals(Tuple(Real(10), Real(10)), (10 - TestDataset.tuple_of_tuples).unwrap.asInstanceOf[Tuple].getVariables(0))
  }
  @Test
  def test_function_of_scalar {
    assertEquals(Sample(Real(0), Real(-1)), (TestDataset.function_of_scalar - 1).unwrap.asInstanceOf[Function].iterator.next)
  }

}