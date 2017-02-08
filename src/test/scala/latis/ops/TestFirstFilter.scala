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
import latis.writer.Writer
import latis.dm.Variable
import latis.writer.AsciiWriter

class TestFirstFilter {
  
  @Test
  def test_canonical = {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val expected = Dataset(Function(Seq(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("myInt"), 1), Real(Metadata("myReal"), 1.1), Text(Metadata("myText"), "A")))), Metadata(Map("length" -> 1.toString))), TestDataset.canonical.getMetadata)
    assertEquals(expected, Operation("first")(TestDataset.canonical))
  }
  
  @Test
  def test_empty = {
    assertEquals(Dataset.empty, Operation("first")(Dataset.empty))
  }
  
  @Test
  def test_scalar = {
    val ds1 = TestDataset.text
    val ds2 = Operation("first")(TestDataset.text)
    assertEquals(ds1, ds2)
  } 
  
//  @Test
//  def test_scalars = {
//    assertEquals(TestDataset.scalars, Operation("first")(TestDataset.scalars))
//  }
  
  @Test
  def test_tuple_of_functions = {
    assertEquals(TestDataset.tuple_of_functions, Operation("first")(TestDataset.tuple_of_functions))
  }
  
  @Test
  def test_function_of_scalars = {
    val ds = TestDataset.function_of_scalar
    val expected = Sample(Real(0), Real(0))
    Operation("first")(ds) match {
      case Dataset(v) => assertEquals(expected, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  
  @Test
  def test_function_of_functions = {
    val ds = TestDataset.function_of_functions
    val expected = Sample(Integer(Metadata("x"), 0), Function((0 until 3).map(j => Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * 0 + j)))))
    Operation("first")(ds) match {
      case Dataset(v) => assertEquals(expected, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  
  @Test
  def test_empty_function = {
    val ds = TestDataset.empty_function
    assertEquals(0, Operation("first")(ds).getLength)
  }
  
  @Test
  def test_metadata {
    Operation("first")(TestDataset.canonical) match {
      case Dataset(v) => assertEquals(Some("1"), v.getMetadata("length"))
      case _ => fail()
    }
  }
}
