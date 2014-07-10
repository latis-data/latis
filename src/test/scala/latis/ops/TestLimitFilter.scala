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
import latis.dm.TestDataset
import latis.dm.Sample
import latis.writer.Writer
import latis.dm.Integer
import latis.dm.Tuple
import latis.dm.Text

class TestLimitFilter {
  
  @Test
  def test_empty = {
    assertEquals(TestDataset.empty, LimitFilter(0)(TestDataset.empty))
    assertEquals(TestDataset.empty, LimitFilter(1)(TestDataset.empty))
    assertEquals(TestDataset.empty, LimitFilter(5)(TestDataset.empty))
  }
  @Test
  def test_real = {
    assertEquals(TestDataset.real, LimitFilter(0)(TestDataset.real))
    assertEquals(TestDataset.real, LimitFilter(1)(TestDataset.real))
    assertEquals(TestDataset.real, LimitFilter(5)(TestDataset.real))
  }
  @Test
  def test_integer = {
    assertEquals(TestDataset.integer, LimitFilter(0)(TestDataset.integer))
    assertEquals(TestDataset.integer, LimitFilter(1)(TestDataset.integer))
    assertEquals(TestDataset.integer, LimitFilter(5)(TestDataset.integer))
  }
  @Test
  def test_text = {
    assertEquals(TestDataset.text, LimitFilter(0)(TestDataset.text))
    assertEquals(TestDataset.text, LimitFilter(1)(TestDataset.text))
    assertEquals(TestDataset.text, LimitFilter(5)(TestDataset.text))
  }
  @Test
  def test_real_time = {
    assertEquals(TestDataset.real_time, LimitFilter(0)(TestDataset.real_time))
    assertEquals(TestDataset.real_time, LimitFilter(1)(TestDataset.real_time))
    assertEquals(TestDataset.real_time, LimitFilter(5)(TestDataset.real_time))
  }
  @Test
  def test_text_time = {
    assertEquals(TestDataset.text_time, LimitFilter(0)(TestDataset.text_time))
    assertEquals(TestDataset.text_time, LimitFilter(1)(TestDataset.text_time))
    assertEquals(TestDataset.text_time, LimitFilter(5)(TestDataset.text_time))
  }
  @Test
  def test_int_time = {
    assertEquals(TestDataset.int_time, LimitFilter(0)(TestDataset.int_time))
    assertEquals(TestDataset.int_time, LimitFilter(1)(TestDataset.int_time))
    assertEquals(TestDataset.int_time, LimitFilter(5)(TestDataset.int_time))
  }
  @Test
  def test_scalars = {
    assertEquals(TestDataset.scalars, LimitFilter(0)(TestDataset.scalars))
    assertEquals(TestDataset.scalars, LimitFilter(1)(TestDataset.scalars))
    assertEquals(TestDataset.scalars, LimitFilter(5)(TestDataset.scalars))
  }
  @Test
  def test_binary = {
    assertEquals(TestDataset.binary, LimitFilter(0)(TestDataset.binary))
    assertEquals(TestDataset.binary, LimitFilter(1)(TestDataset.binary))
    assertEquals(TestDataset.binary, LimitFilter(5)(TestDataset.binary))
  }
  @Test
  def test_tuple_of_scalars = {
    assertEquals(TestDataset.tuple_of_scalars, LimitFilter(0)(TestDataset.tuple_of_scalars))
    assertEquals(TestDataset.tuple_of_scalars, LimitFilter(1)(TestDataset.tuple_of_scalars))
    assertEquals(TestDataset.tuple_of_scalars, LimitFilter(5)(TestDataset.tuple_of_scalars))
  }
  @Test
  def test_tuple_of_tuples = {
    assertEquals(TestDataset.tuple_of_tuples, LimitFilter(0)(TestDataset.tuple_of_tuples))
    assertEquals(TestDataset.tuple_of_tuples, LimitFilter(1)(TestDataset.tuple_of_tuples))
    assertEquals(TestDataset.tuple_of_tuples, LimitFilter(5)(TestDataset.tuple_of_tuples))
  }
//  @Test
//  def test_tuple_of_functions = {
//    assertEquals(TestDataset.tuple_of_functions, LimitFilter(0)(TestDataset.tuple_of_functions))
//    assertEquals(TestDataset.tuple_of_functions, LimitFilter(1)(TestDataset.tuple_of_functions))
//    assertEquals(TestDataset.tuple_of_functions, LimitFilter(5)(TestDataset.tuple_of_functions))
//  }
  @Test
  def test_scalar_tuple = {
    assertEquals(TestDataset.scalar_tuple, LimitFilter(0)(TestDataset.scalar_tuple))
    assertEquals(TestDataset.scalar_tuple, LimitFilter(1)(TestDataset.scalar_tuple))
    assertEquals(TestDataset.scalar_tuple, LimitFilter(5)(TestDataset.scalar_tuple))
  }
//  @Test
//  def test_mixed_tuple = {
//    assertEquals(TestDataset.mixed_tuple, LimitFilter(0)(TestDataset.mixed_tuple))
//    assertEquals(TestDataset.mixed_tuple, LimitFilter(1)(TestDataset.mixed_tuple))
//    assertEquals(TestDataset.mixed_tuple, LimitFilter(5)(TestDataset.mixed_tuple))
//  }
  @Test
  def test_function_of_scalar = {
    val exp1 = Dataset(Function(List(Sample(Real(Metadata("unknown")), Real(Metadata("unknown")))), Metadata(Map("length" -> 0.toString))), TestDataset.function_of_scalar.getMetadata)
    val exp2 = Dataset(Function(List(Sample(Real(0), Real(0))), Metadata(Map("length" -> 1.toString))), TestDataset.function_of_scalar.getMetadata)
    val exp3 = Dataset(Function(List(Sample(Real(0), Real(0)), 
                       Sample(Real(1), Real(1)), 
                       Sample(Real(2), Real(2))), Metadata(Map("length" -> 3.toString))), TestDataset.function_of_scalar.getMetadata)
    assertEquals(exp1, LimitFilter(0)(TestDataset.function_of_scalar))
    assertEquals(exp2, LimitFilter(1)(TestDataset.function_of_scalar))
    assertEquals(exp3, LimitFilter(5)(TestDataset.function_of_scalar))
  }
  @Test
  def test_function_of_tuple = {
    val exp1 = Dataset(Function(List(Sample(Integer(Metadata("unknown")), Tuple(Real(Metadata("unknown")), Text(Metadata("unknown"))))), Metadata(Map("length" -> 0.toString))), TestDataset.function_of_tuple.getMetadata)
    val exp2 = Dataset(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero")))), Metadata(Map("length" -> 1.toString))), TestDataset.function_of_tuple.getMetadata)
    val exp3 = Dataset(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero"))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 1), Text(Metadata("myText"), "one"))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two")))), Metadata(Map("length" -> 3.toString))), TestDataset.function_of_tuple.getMetadata)
    assertEquals(exp1, LimitFilter(0)(TestDataset.function_of_tuple))
    assertEquals(exp2, LimitFilter(1)(TestDataset.function_of_tuple))
    assertEquals(exp3, LimitFilter(5)(TestDataset.function_of_tuple))
  }
  @Test
  def test_function_of_function = {
    val exp1 = Dataset(Function(List(Sample(Integer(Metadata("x")), Function(List(Sample(Integer(Metadata("y")), Real(Metadata("z")))), Metadata(Map("length" -> 0.toString))))), Metadata(Map("length" -> 0.toString, "name" -> "function_of_functions_with_data_in_scalar"))), TestDataset.function_of_functions.getMetadata)
    val exp2 = Dataset(Function(List(Sample(Integer(Metadata("x"), 0), Function(List(Sample(Integer(Metadata("y"), 10), Real(Metadata("z"), 0))), Metadata(Map("length" -> 1.toString))))), Metadata(Map("length" -> 1.toString, "name" -> "function_of_functions_with_data_in_scalar"))), TestDataset.function_of_functions.getMetadata)
    val exp3 = Dataset(Function((0 until 4).map(Integer(Metadata("x"), _)), for (i <- 0 until 4) yield Function((0 until 3).map(j => 
      Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * i + j))), Metadata(Map("length" -> 3.toString))), Metadata(Map("length" -> 4.toString(), "name" -> "function_of_functions_with_data_in_scalar"))), TestDataset.function_of_functions.getMetadata)
    assertEquals(exp1, LimitFilter(0)(TestDataset.function_of_functions))
    assertEquals(exp2, LimitFilter(1)(TestDataset.function_of_functions))
    assertEquals(exp3, LimitFilter(5)(TestDataset.function_of_functions))
  }
  @Test
  def test_mixed_function = {
    val exp1 = Dataset(Function(List(Sample(Real(Metadata("myReal")), Tuple(Tuple(Integer(Metadata("myInteger")), Real(Metadata("myReal"))), Function(List(Sample(Real(Metadata("unknown")), Real(Metadata("unknown")))), Metadata(Map("length" -> 0.toString)))))), Metadata(Map("length" -> 0.toString))), TestDataset.mixed_function.getMetadata)
    val exp2 = Dataset(Function(List(Sample(Real(Metadata("myReal"), 0.0), Tuple(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), (TestDataset.function_of_scalar).getVariables(0)))), Metadata(Map("length" -> 1.toString))), TestDataset.mixed_function.getMetadata)
    val exp3 = Dataset(Function(List(Sample(Real(Metadata("myReal"), 0.0), Tuple(Tuple(Integer(Metadata("myInteger"), 0), Real(Metadata("myReal"), 0)), (TestDataset.function_of_scalar).getVariables(0))),
                       Sample(Real(Metadata("myReal"), 1.1), Tuple(Tuple(Integer(Metadata("myInteger"), 1), Real(Metadata("myReal"), 1)), (TestDataset.function_of_scalar+(1)).getVariables(0))),
                       Sample(Real(Metadata("myReal"), 2.2), Tuple(Tuple(Integer(Metadata("myInteger"), 2), Real(Metadata("myReal"), 2)), (TestDataset.function_of_scalar+(2)).getVariables(0)))), Metadata(Map("length" -> 3.toString))), TestDataset.mixed_function.getMetadata)
    assertEquals(exp1, LimitFilter(0)(TestDataset.mixed_function))
//    assertEquals(exp2, LimitFilter(1)(TestDataset.mixed_function))
//    assertEquals(exp3, LimitFilter(5)(TestDataset.mixed_function))
  }
  @Test
  def test_empty_function = {
    val exp1 = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> 0.toString))), TestDataset.empty_function.getMetadata)
    val exp2 = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> 0.toString))), TestDataset.empty_function.getMetadata)
    val exp3 = Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> 0.toString))), TestDataset.empty_function.getMetadata)
    assertEquals(exp1, LimitFilter(0)(TestDataset.empty_function))
    assertEquals(exp2, LimitFilter(1)(TestDataset.empty_function))
    assertEquals(exp3, LimitFilter(5)(TestDataset.empty_function))
  }
  @Test
  def test_index_function = {
    val exp1 = Dataset(Function(List(Integer(Metadata("unknown"))), Metadata(Map("length" -> 0.toString))), TestDataset.index_function.getMetadata)
    val exp2 = Dataset(Function(List(Integer(1)), Metadata(Map("length" -> 1.toString))), TestDataset.index_function.getMetadata)
    val exp3 = Dataset(Function(List(Integer(1), Integer(2)), Metadata(Map("length" -> 2.toString))), TestDataset.index_function.getMetadata)
    val ds = LimitFilter(0)(TestDataset.index_function)
    assertEquals(exp1, LimitFilter(0)(TestDataset.index_function))
    assertEquals(exp2, LimitFilter(1)(TestDataset.index_function))
    assertEquals(exp3, LimitFilter(5)(TestDataset.index_function))
  }
  @Test
  def test_combo = {
    val exp1 = Dataset(List(Function(List(Sample(Integer(Metadata("unknown")), Tuple(Real(Metadata("unknown")), Text(Metadata("unknown"))))), Metadata(Map("length" -> 0.toString))), TestDataset.tuple_of_tuples.getVariables(0), TestDataset.text.getVariables(0)), TestDataset.combo.getMetadata)
    val exp2 = Dataset(List(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero")))), Metadata(Map("length" -> 1.toString))), TestDataset.tuple_of_tuples.getVariables(0), TestDataset.text.getVariables(0)), TestDataset.combo.getMetadata)
    val exp3 = Dataset(List(Function(List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "zero"))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 1), Text(Metadata("myText"), "one"))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "two")))), Metadata(Map("length" -> 3.toString))), TestDataset.tuple_of_tuples.getVariables(0), TestDataset.text.getVariables(0)), TestDataset.combo.getMetadata)
    assertEquals(exp1, LimitFilter(0)(TestDataset.combo))
    assertEquals(exp2, LimitFilter(1)(TestDataset.combo))
    assertEquals(exp3, LimitFilter(5)(TestDataset.combo))
  }

}