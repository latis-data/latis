package latis.ops

import latis.dm.Function
import latis.dm.implicits._
import latis.dm.Real
import latis.metadata.Metadata
import latis.data.SampledData
import org.junit._
import Assert._
import latis.ops.filter._
import latis.dm.Dataset
import latis.dm.Sample
import latis.dm.TestDataset
import latis.dm.Integer
import latis.dm.Tuple
import latis.dm.Text

class TestStrideFilter { //should a stride of 0 be accepted?
  
  @Test
  def test_empty = {
    assertEquals(TestDataset.empty, StrideFilter(1)(TestDataset.empty))
    assertEquals(TestDataset.empty, StrideFilter(3)(TestDataset.empty))
    assertEquals(TestDataset.empty, StrideFilter(5)(TestDataset.empty))
  }
  @Test
  def test_real = {
    assertEquals(TestDataset.real, StrideFilter(1)(TestDataset.real))
    assertEquals(TestDataset.real, StrideFilter(3)(TestDataset.real))
    assertEquals(TestDataset.real, StrideFilter(5)(TestDataset.real))
  }
  @Test
  def test_integer = {
    assertEquals(TestDataset.integer, StrideFilter(1)(TestDataset.integer))
    assertEquals(TestDataset.integer, StrideFilter(3)(TestDataset.integer))
    assertEquals(TestDataset.integer, StrideFilter(5)(TestDataset.integer))
  }
  @Test
  def test_text = {
    assertEquals(TestDataset.text, StrideFilter(1)(TestDataset.text))
    assertEquals(TestDataset.text, StrideFilter(3)(TestDataset.text))
    assertEquals(TestDataset.text, StrideFilter(5)(TestDataset.text))
  }
  @Test
  def test_real_time = {
    assertEquals(TestDataset.real_time, StrideFilter(1)(TestDataset.real_time))
    assertEquals(TestDataset.real_time, StrideFilter(3)(TestDataset.real_time))
    assertEquals(TestDataset.real_time, StrideFilter(5)(TestDataset.real_time))
  }
  @Test
  def test_text_time = {
    assertEquals(TestDataset.text_time, StrideFilter(1)(TestDataset.text_time))
    assertEquals(TestDataset.text_time, StrideFilter(3)(TestDataset.text_time))
    assertEquals(TestDataset.text_time, StrideFilter(5)(TestDataset.text_time))
  }
  @Test
  def test_int_time = {
    assertEquals(TestDataset.int_time, StrideFilter(1)(TestDataset.int_time))
    assertEquals(TestDataset.int_time, StrideFilter(3)(TestDataset.int_time))
    assertEquals(TestDataset.int_time, StrideFilter(5)(TestDataset.int_time))
  }
  @Test
  def test_scalars = {
    assertEquals(TestDataset.scalars, StrideFilter(1)(TestDataset.scalars))
    assertEquals(TestDataset.scalars, StrideFilter(3)(TestDataset.scalars))
    assertEquals(TestDataset.scalars, StrideFilter(5)(TestDataset.scalars))
  }
  @Test
  def test_binary = {
    assertEquals(TestDataset.binary, StrideFilter(1)(TestDataset.binary))
    assertEquals(TestDataset.binary, StrideFilter(3)(TestDataset.binary))
    assertEquals(TestDataset.binary, StrideFilter(5)(TestDataset.binary))
  }
  @Test
  def test_tuple_of_scalars = {
    assertEquals(TestDataset.tuple_of_scalars, StrideFilter(1)(TestDataset.tuple_of_scalars))
    assertEquals(TestDataset.tuple_of_scalars, StrideFilter(3)(TestDataset.tuple_of_scalars))
    assertEquals(TestDataset.tuple_of_scalars, StrideFilter(5)(TestDataset.tuple_of_scalars))
  }
  @Test
  def test_tuple_of_tuples = {
    assertEquals(TestDataset.tuple_of_tuples, StrideFilter(1)(TestDataset.tuple_of_tuples))
    assertEquals(TestDataset.tuple_of_tuples, StrideFilter(3)(TestDataset.tuple_of_tuples))
    assertEquals(TestDataset.tuple_of_tuples, StrideFilter(5)(TestDataset.tuple_of_tuples))
  }
//  @Test
//  def test_tuple_of_functions = {
//    assertEquals(TestDataset.tuple_of_functions, StrideFilter(1)(TestDataset.tuple_of_functions))
//    assertEquals(TestDataset.tuple_of_functions, StrideFilter(3)(TestDataset.tuple_of_functions))
//    assertEquals(TestDataset.tuple_of_functions, StrideFilter(5)(TestDataset.tuple_of_functions))
//  }
  @Test
  def test_scalar_tuple = {
    assertEquals(TestDataset.scalar_tuple, StrideFilter(1)(TestDataset.scalar_tuple))
    assertEquals(TestDataset.scalar_tuple, StrideFilter(3)(TestDataset.scalar_tuple))
    assertEquals(TestDataset.scalar_tuple, StrideFilter(5)(TestDataset.scalar_tuple))
  }
//  @Test
//  def test_mixed_tuple = {
//    assertEquals(TestDataset.mixed_tuple, StrideFilter(1)(TestDataset.mixed_tuple))
//    assertEquals(TestDataset.mixed_tuple, StrideFilter(3)(TestDataset.mixed_tuple))
//    assertEquals(TestDataset.mixed_tuple, StrideFilter(5)(TestDataset.mixed_tuple))
//  }
  @Test
  def test_function_of_scalar = {
    val exp1 = Dataset(Function(List(Sample(Real(0), Real(0)), 
                       Sample(Real(1), Real(1)), 
                       Sample(Real(2), Real(2))), Metadata(Map("length" -> 3.toString))), TestDataset.function_of_scalar.getMetadata)
    val exp2 = Dataset(Function(List(Sample(Real(0), Real(0))), Metadata(Map("length" -> 1.toString))), TestDataset.function_of_scalar.getMetadata)
    assertEquals(exp1, StrideFilter(1)(TestDataset.function_of_scalar))
    assertEquals(exp2, StrideFilter(3)(TestDataset.function_of_scalar))
    assertEquals(exp2, StrideFilter(5)(TestDataset.function_of_scalar))
  }
  @Test
  def test_function_of_tuple = {
    val exp2 = Dataset(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero")))), Metadata(Map("length" -> 1.toString))), TestDataset.function_of_tuple.getMetadata)
    val exp1 = Dataset(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero"))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 1), Text(Metadata("myText"), "one"))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two")))), Metadata(Map("length" -> 3.toString))), TestDataset.function_of_tuple.getMetadata)
    assertEquals(exp1, StrideFilter(1)(TestDataset.function_of_tuple))
    assertEquals(exp2, StrideFilter(3)(TestDataset.function_of_tuple))
    assertEquals(exp2, StrideFilter(5)(TestDataset.function_of_tuple))
  }
  @Test
  def test_function_of_function = {
    val exp2 = Dataset(Function(List(Sample(Integer(Metadata("x"), 0), Function(List(Sample(Integer(Metadata("y"), 10), Real(Metadata("z"), 0))), Metadata(Map("length" -> 1.toString))))), Metadata(Map("length" -> 1.toString, "name" -> "function_of_functions_with_data_in_scalar"))), TestDataset.function_of_functions.getMetadata)
    val exp1 = Dataset(Function((0 until 4).map(Integer(Metadata("x"), _)), for (i <- 0 until 4) yield Function((0 until 3).map(j => 
      Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * i + j))), Metadata(Map("length" -> 3.toString))), Metadata(Map("length" -> 4.toString(), "name" -> "function_of_functions_with_data_in_scalar"))), TestDataset.function_of_functions.getMetadata)
    assertEquals(exp1, StrideFilter(1)(TestDataset.function_of_functions))
    assertEquals(exp2, StrideFilter(4)(TestDataset.function_of_functions))
    assertEquals(exp2, StrideFilter(5)(TestDataset.function_of_functions))
  }
  @Test
  def test_mixed_function = {
    val exp2 = Dataset(Function(List(Sample(Real(Metadata("myReal"), 0.0), Tuple(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), (TestDataset.function_of_scalar).getVariables(0)))), Metadata(Map("length" -> 1.toString))), TestDataset.mixed_function.getMetadata)
    val exp1 = Dataset(Function(List(Sample(Real(Metadata("myReal"), 0.0), Tuple(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), (TestDataset.function_of_scalar).getVariables(0))),
                       Sample(Real(Metadata("myReal"), 1.1), Tuple(Tuple(Integer(Metadata("myInteger"), 1), Real(Metadata("myReal"), 1)), (TestDataset.function_of_scalar+(1)).getVariables(0))),
                       Sample(Real(Metadata("myReal"), 2.2), Tuple(Tuple(Integer(Metadata("myInteger"), 2), Real(Metadata("myReal"), 2)), (TestDataset.function_of_scalar+(2)).getVariables(0)))), Metadata(Map("length" -> 3.toString))), TestDataset.mixed_function.getMetadata)
    assertEquals(exp1, StrideFilter(1)(TestDataset.mixed_function))
    assertEquals(exp2, StrideFilter(3)(TestDataset.mixed_function))
    assertEquals(exp2, StrideFilter(5)(TestDataset.mixed_function))
  }
  @Test
  def test_empty_function = {
    val exp2 = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> 0.toString))), TestDataset.empty_function.getMetadata)
    val exp1 = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> 0.toString))), TestDataset.empty_function.getMetadata)
    assertEquals(exp1, StrideFilter(1)(TestDataset.empty_function))
    assertEquals(exp2, StrideFilter(3)(TestDataset.empty_function))
    assertEquals(exp2, StrideFilter(5)(TestDataset.empty_function))
  }
  @Test
  def test_index_function = {
    val exp2 = Dataset(Function(List(Integer(1)), Metadata(Map("length" -> 1.toString))), TestDataset.index_function.getMetadata)
    val exp1 = Dataset(Function(List(Integer(1), Integer(2)), Metadata(Map("length" -> 2.toString))), TestDataset.index_function.getMetadata)
    assertEquals(exp1, StrideFilter(1)(TestDataset.index_function))
    assertEquals(exp2, StrideFilter(3)(TestDataset.index_function))
    assertEquals(exp2, StrideFilter(5)(TestDataset.index_function))
  }
  @Test
  def test_combo = {
    val exp2 = Dataset(List(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero")))), Metadata(Map("length" -> 1.toString))), TestDataset.tuple_of_tuples.getVariables(0), TestDataset.text.getVariables(0)), TestDataset.combo.getMetadata)
    val exp1 = Dataset(List(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero"))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 1), Text(Metadata("myText"), "one"))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two")))), Metadata(Map("length" -> 3.toString))), TestDataset.tuple_of_tuples.getVariables(0), TestDataset.text.getVariables(0)), TestDataset.combo.getMetadata)
    assertEquals(exp1, StrideFilter(1)(TestDataset.combo))
    assertEquals(exp2, StrideFilter(3)(TestDataset.combo))
    assertEquals(exp2, StrideFilter(5)(TestDataset.combo))
  }

}