package latis.ops

import latis.dm.TestDataset
import latis.dm.Integer
import latis.dm.Function
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.dm.Real
import latis.dm.Sample
import org.junit.Assert._
import latis.time.Time
import latis.dm.Text
import latis.dm.Index
import latis.dm.implicits._
import latis.dm.AbstractTuple
import latis.writer.Writer

class TestAddition {
  
  @Test
  def test_empty {
    assertEquals(Dataset.empty, Dataset.empty + 899)
  }
  @Test
  def test_real_value {
    (3.14 + TestDataset.real) match {
      case Dataset(v) => assertEquals(6.28, v.getNumberData.doubleValue, 0.0)
      case _ => fail()
    }
  }
  @Test
  def test_real_md {
    assertEquals("realDS", (TestDataset.real + 3.14).getName)
  }
  @Test
  def test_integer_value {
    ((-2) + TestDataset.integer) match {
      case Dataset(v) => assertEquals(40, v.getNumberData.intValue)
      case _ => fail()
    }
  }
  @Test
  def test_integer_md {
    assertEquals("intDS", (TestDataset.integer + (-2)).getName)
  }
  @Test
  def test_text {
    (TestDataset.text + 1) match {
      case Dataset(v) => assertEquals(Double.NaN, v.getNumberData.doubleValue, 0.0)
      case _ => fail()
    }
  }
  @Test
  def test_time {
    (TestDataset.real_time + 500) match {
      case Dataset(v) => assertEquals(1500.0, v.getNumberData.doubleValue, 0.0)
      case _ => fail()
    }
  }
  @Test
  def test_tuple_of_tuples {
    (10 + TestDataset.tuple_of_tuples) match {
      case Dataset(v) => assertEquals(Tuple(Real(10), Real(10)), v.asInstanceOf[Tuple].getVariables(0))
      case _ => fail()
    }
  }
  @Test
  def test_function_of_scalar {
    (TestDataset.function_of_scalar + 1) match {
      case Dataset(v) => assertEquals(Sample(Real(0), Real(1)), v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }

}
