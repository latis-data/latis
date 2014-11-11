package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.implicits._
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.TestDataset
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.ops.filter.StrideFilter
import latis.time.Time
import latis.writer.AsciiWriter

class TestStrideFilter {
  
  @Test
  def test_canonical_with_stride_1 {
    assertEquals(TestDataset.canonical, StrideFilter(1)(TestDataset.canonical))
  }
  @Test
  def test_canonical_with_stride_3 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(List(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("myInt"), 1), Real(Metadata("myReal"), 1.1), Text(Metadata("myText"), "A"))))), TestDataset.canonical.getMetadata)
    assertEquals(exp, StrideFilter(3)(TestDataset.canonical))
  }
  @Test
  def test_canonical_with_stride_5 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(List(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("myInt"), 1), Real(Metadata("myReal"), 1.1), Text(Metadata("myText"), "A"))))), TestDataset.canonical.getMetadata)
    assertEquals(exp, StrideFilter(5)(TestDataset.canonical))
  }
  @Test
  def test_stride_0 {
    try {
      StrideFilter(0)(TestDataset.canonical)
      fail("stride(0) should throw an exception")
    }
    catch {case e: Exception => }
  }
  @Test
  def test_empty {
    assertEquals(TestDataset.empty, StrideFilter(3)(TestDataset.empty))
  }
  @Test
  def test_scalar {
    assertEquals(TestDataset.real, StrideFilter(3)(TestDataset.real))
  }
  @Test
  def test_scalars {
    assertEquals(TestDataset.scalars, StrideFilter(3)(TestDataset.scalars))
  }
  @Test
  def test_tuple {
    assertEquals(TestDataset.tuple_of_tuples, StrideFilter(3)(TestDataset.tuple_of_tuples))
  }
  @Test
  def test_tuple_of_functions {
    assertEquals(TestDataset.tuple_of_functions, StrideFilter(3)(TestDataset.tuple_of_functions))
  }
  @Test
  def test_function_of_scalar_length_with_stride_1 {
    assertEquals(3, StrideFilter(1)(TestDataset.function_of_scalar).getLength)
  }
  @Test
  def test_function_of_scalar_length_with_stride_3 {
    assertEquals(1, StrideFilter(3)(TestDataset.function_of_scalar).getLength)    
  }
  @Test
  def test_function_of_scalar_with_stride_5 {
    assertEquals(Sample(Real(0), Real(0)), StrideFilter(3)(TestDataset.function_of_scalar).findFunction.get.iterator.next)    
  }
  @Test
  def test_function_of_function_length_with_stride_2 {
    assertEquals(2, StrideFilter(2)(TestDataset.function_of_functions).getLength)
  }
  @Test
  def test_function_of_function_with_stride_2 {
    val s = Sample(Integer(Metadata("x"), 2), Function((0 until 3).map(j => Sample(Integer(Metadata("y"), 10+j), Real(Metadata("z"), 20+j)))))
    val it = StrideFilter(2)(TestDataset.function_of_functions).findFunction.get.iterator
    it.next
    assertEquals(s, it.next)
  }
  @Test
  def test_empty_function {
    assertEquals(TestDataset.empty_function, StrideFilter(1)(TestDataset.empty_function))
  }
  @Test
  def test_metadata_change_stride_2 {
    assertEquals(Some("2"), StrideFilter(2)(TestDataset.function_of_scalar_with_length).findFunction.get.getMetadata("length"))
  }
  @Test
  def test_metadata_change_stride_5 {
    assertEquals(Some("1"), StrideFilter(5)(TestDataset.function_of_scalar_with_length).findFunction.get.getMetadata("length"))
  }
}