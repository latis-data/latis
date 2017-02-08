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
import latis.dm.Index
import latis.time.Time

class TestLimitFilter {
  
  @Test
  def test_canonical_with_limit_0 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(Time(Metadata(md)), Tuple(Integer(Metadata("myInt")), Real(Metadata("myReal")), Text(Metadata("myText"))), Iterator.empty, Metadata(Map("length" -> 0.toString))), TestDataset.canonical.getMetadata)
    assertEquals(exp, LimitFilter(0)(TestDataset.canonical))
  }
  @Test
  def test_canonical_with_limit_5 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(List(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("myInt"), 1), Real(Metadata("myReal"), 1.1), Text(Metadata("myText"), "A"))),
                       Sample(Time(Metadata(md), "1970/01/02"), Tuple(Integer(Metadata("myInt"), 2), Real(Metadata("myReal"), 2.2), Text(Metadata("myText"), "B"))),
                       Sample(Time(Metadata(md), "1970/01/03"), Tuple(Integer(Metadata("myInt"), 3), Real(Metadata("myReal"), 3.3), Text(Metadata("myText"), "C")))), Metadata(Map("length" -> 3.toString))), TestDataset.canonical.getMetadata)
    assertEquals(exp, LimitFilter(5)(TestDataset.canonical))
  }
  @Test
  def test_canonical_with_limit_2 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(List(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("myInt"), 1), Real(Metadata("myReal"), 1.1), Text(Metadata("myText"), "A"))),
                       Sample(Time(Metadata(md), "1970/01/02"), Tuple(Integer(Metadata("myInt"), 2), Real(Metadata("myReal"), 2.2), Text(Metadata("myText"), "B")))), Metadata(Map("length" -> 2.toString))), TestDataset.canonical.getMetadata)
    assertEquals(exp, LimitFilter(2)(TestDataset.canonical))
  }
  @Test
  def test_empty {
    assertEquals(Dataset.empty, LimitFilter(5)(Dataset.empty))
  }
  @Test
  def test_scalar {
    assertEquals(TestDataset.integer, LimitFilter(0)(TestDataset.integer))
  }
//  @Test
//  def test_scalars {
//    assertEquals(TestDataset.scalars, LimitFilter(1)(TestDataset.scalars))
//  }
  @Test
  def test_tuple_of_scalars {
    assertEquals(TestDataset.tuple_of_scalars, LimitFilter(5)(TestDataset.tuple_of_scalars))
  }
  @Test
  def test_tuple_of_functions {
    assertEquals(TestDataset.tuple_of_functions, LimitFilter(1)(TestDataset.tuple_of_functions))
  }
  @Test
  def test_function_of_scalar_with_limit_0 {
    assertEquals(0, LimitFilter(0)(TestDataset.function_of_scalar).getLength)
  }
  @Test
  def test_function_of_scalar_with_limit_1 {
    assertEquals(1, LimitFilter(1)(TestDataset.function_of_scalar).getLength)
  }
  @Test
  def test_function_of_scalar_with_limit_5 {
    assertEquals(3, LimitFilter(5)(TestDataset.function_of_scalar).getLength)
  }
  @Test
  def test_function_of_function_with_limit_1 {
    val exp = Sample(Integer(Metadata("x"), 0), Function((0 until 3).map(j => Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * 0 + j)))))
    LimitFilter(1)(TestDataset.function_of_functions) match {
      case Dataset(v) => assertEquals(exp, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  @Test
  def test_empty_function {
    assertEquals(Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> "0"))), Metadata("empty_function")), LimitFilter(5)(TestDataset.empty_function))
  }
  @Test
  def test_metadata_length {
    LimitFilter(2)(TestDataset.function_of_scalar_with_length) match {
      case Dataset(v) => assertEquals(Some("2"), v.asInstanceOf[Function].getMetadata("length"))
      case _ => fail()
    }
  }

}
