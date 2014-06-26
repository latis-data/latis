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

class TestFirstFilter {
  
  @Test
  def test_empty = {
    assertEquals(TestDataset.empty, Operation("first")(TestDataset.empty))
  }
  
  @Test
  def test_real = {
    assertEquals(TestDataset.real, Operation("first")(TestDataset.real))
  }
  
  @Test
  def test_integer = {
    assertEquals(TestDataset.integer, Operation("first")(TestDataset.integer))
  }
  
  @Test
  def test_text = {
    assertEquals(TestDataset.text, Operation("first")(TestDataset.text))
  } 
  
  @Test
  def test_real_time = {
    assertEquals(TestDataset.real_time, Operation("first")(TestDataset.real_time))
  }
  
  @Test
  def test_text_time = {
    assertEquals(TestDataset.text_time, Operation("first")(TestDataset.text_time))
  }
  
  @Test
  def test_int_time = {
    assertEquals(TestDataset.int_time, Operation("first")(TestDataset.int_time))
  }
  
  @Test
  def test_scalars = {
    assertEquals(TestDataset.scalars, Operation("first")(TestDataset.scalars))
  }
  
  @Test
  def test_binary = {
    assertEquals(TestDataset.binary, Operation("first")(TestDataset.binary))
  }
  
  @Test
  def test_tuple_of_scalar = {
    assertEquals(TestDataset.tuple_of_scalars, Operation("first")(TestDataset.tuple_of_scalars))
  }
  
  @Test
  def test_tuple_of_tuple = {
    assertEquals(TestDataset.tuple_of_tuples, Operation("first")(TestDataset.tuple_of_tuples))
  }
  
  @Test
  def test_tuple_of_functions = {
    assertEquals(TestDataset.tuple_of_functions, Operation("first")(TestDataset.tuple_of_functions))
  }
  
  @Test
  def test_scalar_tuple = {
    assertEquals(TestDataset.scalar_tuple, Operation("first")(TestDataset.scalar_tuple))
  }
  
  @Test
  def test_mixed_tuple = {
    assertEquals(TestDataset.mixed_tuple, Operation("first")(TestDataset.mixed_tuple))
  }
  
  @Test
  def test_function_of_scalars = {
    val ds = TestDataset.function_of_scalar_with_iterable_data
    val expected = Dataset(Sample(Real(0), Real(0)), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_function_of_tuples = {
    val ds = TestDataset.function_of_tuple_with_iterable_data
    val expected = Dataset(Sample(Real(0), Tuple(Real(0), Real(10))), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_function_of_functions = {
    val ds = TestDataset.function_of_functions
    val expected = Dataset(Sample(Integer(Metadata("x"), 0), Function((0 until 3).map(j => Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * 0 + j))))), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_mixed_function = {
    val ds = TestDataset.mixed_function
    val expected = Dataset(Sample(Real(Metadata("myReal"), 0.0), Tuple(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), TestDataset.function_of_scalar_with_iterable_data.getVariables(0))), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_scalar_time_series = {
    val ds = TestDataset.time_series_of_scalar
    val expected = Dataset(Sample(Time(Metadata("myTime"), 1000.0), Integer(Metadata("myInteger"), 3)), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_tuple_time_series = {
    val ds = TestDataset.time_series_of_tuple
    val md = Map("name" -> "time", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val expected = Dataset(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("int"), 1), Real(Metadata("real"), 1.1), Text(Metadata("text"), "A"))), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_empty_function = {
    val ds = TestDataset.empty_function
    val expected = Dataset(Sample(Real(Metadata("domain")), Real(Metadata("range"))), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_index_function = {
    val ds = TestDataset.index_function
    val expected = Dataset(Sample(Index(0), Integer(1)), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
  @Test
  def test_combo = {
    val ds = TestDataset.combo
    val expected = Dataset(List(Sample(Real(0), Tuple(Real(0), Real(10))), TestDataset.tuple_of_tuples.getVariables(0), TestDataset.text.getVariables(0)), ds.getMetadata)
    assertEquals(expected, Operation("first")(ds))
  }
  
}