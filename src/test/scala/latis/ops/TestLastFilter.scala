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

class TestLastFilter {
  
  @Test
  def test_canonical = {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val expected = Dataset(Function(Seq(Sample(Time(Metadata(md), "1970/01/03"), Tuple(Integer(Metadata("myInt"), 3), Real(Metadata("myReal"), 3.3), Text(Metadata("myText"), "C")))), Metadata(Map("length" -> 1.toString))), TestDataset.canonical.getMetadata)
    assertEquals(expected, Operation("last")(TestDataset.canonical))
  }
  
  @Test
  def test_empty = {
    assertEquals(TestDataset.empty, Operation("last")(TestDataset.empty))
  }
  
  @Test
  def test_real = {
    assertEquals(TestDataset.real, Operation("last")(TestDataset.real))
  }
  
  @Test
  def test_integer = {
    assertEquals(TestDataset.integer, Operation("last")(TestDataset.integer))
  }
  
  @Test
  def test_text = {
    assertEquals(TestDataset.text, Operation("last")(TestDataset.text))
  } 
  
  @Test
  def test_real_time = {
    assertEquals(TestDataset.real_time, Operation("last")(TestDataset.real_time))
  }
  
  @Test
  def test_text_time = {
    assertEquals(TestDataset.text_time, Operation("last")(TestDataset.text_time))
  }
  
  @Test
  def test_int_time = {
    assertEquals(TestDataset.int_time, Operation("last")(TestDataset.int_time))
  }
  
  @Test
  def test_scalars = {
    assertEquals(TestDataset.scalars, Operation("last")(TestDataset.scalars))
  }
  
  @Test
  def test_binary = {
    assertEquals(TestDataset.binary, Operation("last")(TestDataset.binary))
  }
  
  @Test
  def test_tuple_of_scalar = {
    assertEquals(TestDataset.tuple_of_scalars, Operation("last")(TestDataset.tuple_of_scalars))
  }
  
  @Test
  def test_tuple_of_tuple = {
    assertEquals(TestDataset.tuple_of_tuples, Operation("last")(TestDataset.tuple_of_tuples))
  }
  
  @Test
  def test_tuple_of_functions = {
    assertEquals(TestDataset.tuple_of_functions, Operation("last")(TestDataset.tuple_of_functions))
  }
  
  @Test
  def test_scalar_tuple = {
    assertEquals(TestDataset.scalar_tuple, Operation("last")(TestDataset.scalar_tuple))
  }
  
  @Test
  def test_mixed_tuple = {
    assertEquals(TestDataset.mixed_tuple, Operation("last")(TestDataset.mixed_tuple))
  }
  
  @Test
  def test_function_of_scalars = {
    val ds = TestDataset.function_of_scalar
    val expected = Dataset(Function(Seq(Sample(Real(2), Real(2))), Metadata(Map("length" -> 1.toString))), ds.getMetadata)
    assertEquals(expected, Operation("last")(ds))
  }
  
  @Test
  def test_function_of_tuples = {
    val ds = TestDataset.function_of_tuple
    val expected = Dataset(Function(Seq(Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two")))), Metadata(Map("length" -> 1.toString))), ds.getMetadata)
    assertEquals(expected, Operation("last")(ds))
  }
  
  @Test
  def test_function_of_functions = {
    val ds = TestDataset.function_of_functions
    val expected = Dataset(Function(Seq(Sample(Integer(Metadata("x"), 3), Function((0 until 3).map(j => Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * 3 + j)))))), Metadata(Map("name" -> "function_of_functions_with_data_in_scalar", "length" -> 1.toString))), ds.getMetadata)
    assertEquals(expected, Operation("last")(ds))
  }
  
  @Test
  def test_mixed_function = {
    val ds = TestDataset.mixed_function
    val expected = Dataset(Function(Seq(Sample(Real(Metadata("myReal"), 2.2), Tuple(Tuple(Integer(Metadata("myInteger"), 2), Real(Metadata("myReal"), 2)), (TestDataset.function_of_scalar+(Dataset(Real(2)))).getVariables(0)))), Metadata(Map("length" -> 1.toString))), ds.getMetadata)
    assertEquals(expected, Operation("last")(ds))
  }
  
  @Test
  def test_empty_function = {
    val ds = TestDataset.empty_function
    val expected = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> 0.toString))), ds.getMetadata)
    assertEquals(expected, Operation("last")(ds))
  }
  
  @Test
  def test_index_function = {
    val ds = TestDataset.index_function
    val expected = Dataset(Function(Seq(Sample(Index(1), Integer(2))), Metadata(Map("length" -> 1.toString))), ds.getMetadata)
    assertEquals(expected, Operation("last")(ds))
  }
  
  @Test
  def test_combo = {
    val ds = TestDataset.combo
    val expected = Dataset(List(Function(Seq(Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two")))), Metadata(Map("length" -> 1.toString))), TestDataset.tuple_of_tuples.getVariables(0), TestDataset.text.getVariables(0)), ds.getMetadata)
    assertEquals(expected, Operation("last")(ds))
  }

}