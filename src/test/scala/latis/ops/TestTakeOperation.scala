package latis.ops

import scala.collection.mutable.ArrayBuffer
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
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestTakeOperation {
  
  @Test
  def test_canonical_with_take_0 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(Time(Metadata(md)), Tuple(Integer(Metadata("myInt")), Real(Metadata("myReal")), Text(Metadata("myText"))), Iterator.empty, Metadata(Map("length" -> 0.toString))), TestDataset.canonical.getMetadata)
    assertEquals(exp, TakeOperation(0)(TestDataset.canonical))
  }
  @Test
  def test_canonical_with_take_5 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(List(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("myInt"), 1), Real(Metadata("myReal"), 1.1), Text(Metadata("myText"), "A"))),
                       Sample(Time(Metadata(md), "1970/01/02"), Tuple(Integer(Metadata("myInt"), 2), Real(Metadata("myReal"), 2.2), Text(Metadata("myText"), "B"))),
                       Sample(Time(Metadata(md), "1970/01/03"), Tuple(Integer(Metadata("myInt"), 3), Real(Metadata("myReal"), 3.3), Text(Metadata("myText"), "C")))), Metadata(Map("length" -> 3.toString))), TestDataset.canonical.getMetadata)
    assertEquals(exp, TakeOperation(5)(TestDataset.canonical))
  }
  @Test
  def test_canonical_with_take_2 {
    val md = Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd")
    val exp = Dataset(Function(List(Sample(Time(Metadata(md), "1970/01/01"), Tuple(Integer(Metadata("myInt"), 1), Real(Metadata("myReal"), 1.1), Text(Metadata("myText"), "A"))),
                       Sample(Time(Metadata(md), "1970/01/02"), Tuple(Integer(Metadata("myInt"), 2), Real(Metadata("myReal"), 2.2), Text(Metadata("myText"), "B")))), Metadata(Map("length" -> 2.toString))), TestDataset.canonical.getMetadata)
    assertEquals(exp, TakeOperation(2)(TestDataset.canonical))
  }
  @Test
  def test_empty {
    assertEquals(Dataset.empty, TakeOperation(5)(Dataset.empty))
  }
  @Test
  def test_scalar {
    assertEquals(TestDataset.integer, TakeOperation(0)(TestDataset.integer))
  } 
  @Test
  def test_tuple_of_scalars {
    assertEquals(TestDataset.tuple_of_scalars, TakeOperation(5)(TestDataset.tuple_of_scalars))
  } 
  @Test
  def test_tuple_of_functions {
    assertEquals(TestDataset.tuple_of_functions, TakeOperation(1)(TestDataset.tuple_of_functions))
  }
  @Test
  def test_function_of_scalar_with_take_1 {
    assertEquals(1, TakeOperation(1)(TestDataset.function_of_scalar).getLength)
  }
  @Test
  def test_function_of_scalar_with_take_5 {
    assertEquals(3, TakeOperation(5)(TestDataset.function_of_scalar).getLength)
  }
  @Test
  def test_function_of_function_with_take_1 {
    val exp = Sample(Integer(Metadata("x"), 0), Function((0 until 3).map(j => Sample(Integer(Metadata("y"), 10 + j), Real(Metadata("z"), 10 * 0 + j)))))
    assertEquals(exp, TakeOperation(1)(TestDataset.function_of_functions).unwrap.asInstanceOf[Function].iterator.next)
  }
  @Test
  def test_empty_function {
    assertEquals(Dataset(Function(Real(Metadata("domain")), Real(Metadata("range")), Iterator.empty, Metadata(Map("length" -> "0"))), Metadata("empty_function")), TakeOperation(5)(TestDataset.empty_function))
  }
  @Test
  def test_metadata_length {
    assertEquals(Some("2"), TakeOperation(2)(TestDataset.function_of_scalar_with_length).unwrap.asInstanceOf[Function].getMetadata("length"))
  }
  
  @Test
  def test_tsml_data {
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset
    val ds = TakeOperation(2)(data)
    ds match {
      case Dataset(x) => x match {
        case Function(f) => f.toList.last match {
          case Sample(Real(r1),Real(r2)) => {
            assertEquals(1620.5,r1,0)
            assertEquals(1360.4894,r2,0)
          }
        }
      }
    }
  }
  
  @Test
  def test_tsml_data_with_ops {
    val ops = ArrayBuffer[Operation]()
    ops += Operation("take",List("2"))
    //val ds1 = TsmlReader("datasets/test/data_with_marker.tsml").getDataset
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset(ops)
    data match {
      case Dataset(x) => x match {
        case Function(f) => f.toList.last match {
          case Sample(Real(r1),Real(r2)) => {
            assertEquals(1620.5,r1,0)
            assertEquals(1360.4894,r2,0)
          }
        }
      }
    }
  }

}