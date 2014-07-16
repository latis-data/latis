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

class TestSubtraction {
  
  @Test
  def test_empty = {
    assertEquals(TestDataset.empty, TestDataset.empty - 899)
    //assertEquals(Dataset(List(Dataset(List()))), 899 + TestDataset.empty)
  }
  @Test
  def test_real = {
    assertEquals(Dataset(Real(0), TestDataset.real.getMetadata), TestDataset.real - 3.14)
    assertEquals(Dataset(Real(0)), 3.14 - TestDataset.real)

  }
  @Test
  def test_integer = {
    assertEquals(Dataset(Real(44), TestDataset.integer.getMetadata), TestDataset.integer - (-2))
    assertEquals(Dataset(Real(-44)), (-2) - TestDataset.integer)
  }
  //@Test
  def test_text = {
    assertEquals(Dataset(Real(Double.NaN)), Dataset(Real(Double.NaN))) // Dataset.equals seems to not work for NaNs
    //assertEquals(Dataset(Real(Double.NaN), TestDataset.text.getMetadata), TestDataset.text + 1)
  }
  @Test
  def test_time = {
    assertEquals(Dataset(Real(500.0), TestDataset.real_time.getMetadata), TestDataset.real_time - 500)
    assertEquals(Dataset(Real(-500.0)), 500 - TestDataset.real_time)
  }
  //@Test
  def test_binary = {
    assertEquals(Dataset(Real(2.2)), TestDataset.binary + TestDataset.binary)
  }
  //@Test
  def test_tuple_of_scalars = {
    assertEquals(Dataset(Tuple(Real(3), Real(3), Real(Double.NaN)), TestDataset.tuple_of_scalars.getMetadata), TestDataset.tuple_of_scalars + 3)
    assertEquals(Dataset(Tuple(Real(3), Real(3), Real(Double.NaN))), 3 + TestDataset.tuple_of_scalars)
  }
  @Test
  def test_tuple_of_tuples = {
    assertEquals(Dataset(Tuple(Tuple(Real(-10), Real(-10)), Tuple(Real(-9), Real(-8.9))), TestDataset.tuple_of_tuples.getMetadata), TestDataset.tuple_of_tuples - 10)
    assertEquals(Dataset(Tuple(Tuple(Real(10), Real(10)), Tuple(Real(9), Real(8.9)))), 10 - TestDataset.tuple_of_tuples)
  }
  //@Test
  def test_tuple_of_functions = {
    val exp1 = Dataset(Tuple(for (i <- 0 until 4) yield Function((0 until 3).map(j =>
      Sample(Integer(Metadata("myInt"+i), 10 + j), Real(Metadata("myReal"+i), 10 * i + j - 7))))), TestDataset.tuple_of_functions.getMetadata)
    assertEquals(exp1, TestDataset.tuple_of_functions - 7)
  }
  @Test
  def test_function_of_scalar = {
    val exp1 = Dataset(Function(List(Sample(Real(0), Real(-1)), 
                       Sample(Real(1), Real(0)), 
                       Sample(Real(2), Real(1)))), TestDataset.function_of_scalar.getMetadata)
    val exp2 = Dataset(Function(List(Sample(Real(0), Real(1)), 
                       Sample(Real(1), Real(0)), 
                       Sample(Real(2), Real(-1)))))
    assertEquals(exp1, TestDataset.function_of_scalar - 1)
    assertEquals(exp2, 1 - TestDataset.function_of_scalar)
  }
  //@Test
  def test_function_of_tuple = {
    val exp1 = Dataset(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 9), Real(Metadata("myText"), Double.NaN))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 10), Real(Metadata("myText"), Double.NaN))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 11), Real(Metadata("myText"), Double.NaN))))), TestDataset.function_of_tuple.getMetadata)
    assertEquals(exp1, TestDataset.function_of_tuple + 9)
  }
  //@Test
  def Test_function_of_function = {
    
  }
  @Test
  def test_empty_function = {
    val exp1 = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty), TestDataset.empty_function.getMetadata)
    val exp2 = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty))
    assertEquals(exp1, TestDataset.empty_function - 99)
    assertEquals(exp2, 99 - TestDataset.empty_function)
  }
  //@Test
  def test_index_function = {
    val exp = Dataset(Function(List(Integer(2), Integer(3))), TestDataset.index_function.getMetadata)
    assertEquals(exp, TestDataset.index_function - (-1))
  }

}