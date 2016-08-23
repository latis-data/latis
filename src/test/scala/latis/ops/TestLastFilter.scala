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
    assertEquals(Dataset.empty, Operation("last")(Dataset.empty))
  }
  
  @Test
  def test_scalar = {
    assertEquals(TestDataset.text, Operation("last")(TestDataset.text))
  } 
  
//  @Test
//  def test_scalars = {
//    assertEquals(TestDataset.scalars, Operation("last")(TestDataset.scalars))
//  }
  
  @Test
  def test_tuple_of_functions = {
    assertEquals(TestDataset.tuple_of_functions, Operation("last")(TestDataset.tuple_of_functions))
  }
  
  @Test
  def test_function_of_scalars = {
    val ds = TestDataset.function_of_scalar
    val s = Sample(Real(2), Real(2))
    Operation("last")(ds) match {
      case Dataset(v) => assertEquals(s, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  
  @Test
  def test_function_of_functions = {
    val ds = TestDataset.function_of_functions
    val expected = Sample(Integer(Metadata("x"), 3), Function((0 until 3).map(j => Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * 3 + j)))))
    Operation("last")(ds) match {
      case Dataset(v) => assertEquals(expected, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  
  @Test
  def test_empty_function = {
    val ds = TestDataset.empty_function
    assertEquals(0, Operation("last")(ds).getLength)
  }
  
  @Test
  def test_metadata {
    Operation("last")(TestDataset.canonical) match {
      case Dataset(v) => assertEquals(Some("1"), v.asInstanceOf[Function].getMetadata("length"))
      case _ => fail()
    }
  }
}
