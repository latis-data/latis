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
import latis.dm.Variable
import latis.writer.AsciiWriter


class TestTakeRightOperation {
 
  @Test
  def test_takeright_0 {
    val ds = TakeRightOperation(0)(TestDataset.canonical)
    ds match {
      case Dataset(Function(s)) => assertEquals(true, s.isEmpty)
    }
  }
  
  @Test
  def test_takeright_neg {
    val ds = TakeRightOperation(-1)(TestDataset.canonical)
    ds match {
      case Dataset(Function(s)) => assertEquals(true, s.isEmpty)
    }
  }
  
  @Test
  def test_takeright_1 {
    val ds = TakeRightOperation(1)(TestDataset.canonical)
    ds match {
      case Dataset(Function(s)) => s.toList.head match {
        case Sample(_,Tuple(x)) => x match {
          case Seq(Integer(i),Real(r),Text(t)) => {
            assertEquals(3,i)
            assertEquals(3.3,r,0)
            assertEquals("C",t)
          }
        }
      }
    }
  }
  
  @Test
  def test_takeright_2 {
    val ds = TakeRightOperation(2)(TestDataset.canonical)
    ds match {
      case Dataset(Function(s)) => s.toList.head match {
        case Sample(_,Tuple(x)) => x match {
          case Seq(Integer(i),Real(r),Text(t)) => {
            assertEquals(2,i)
            assertEquals(2.2,r,0)
            assertEquals("B",t)
          }
        }
      }
    }
  }
  
  @Test
  def test_takeright_empty_dataset {
    val ds = TakeRightOperation(5)(Dataset.empty)
    assertEquals(Dataset.empty, ds)
  }
  
  @Test
  def test_should_not_takeright_scalar {
    val ds = TakeRightOperation(1)(TestDataset.integer)
    ds match {
      case Dataset(x) => x match {
        case Integer(i) => assertEquals(42,i)
        case _ => fail
      }
      case _ => fail
    }
  }
  
  @Test
  def test_should_not_takeright_tuple_of_scalars {
    val ds = TakeRightOperation(5)(TestDataset.tuple_of_scalars)
    ds match {
      case Dataset(x) => x match {
        case Tuple(y) => y.head match {
          case Integer(i) => assertEquals(0,i)
          case _ => fail
        }
      }
    }
  }
  
  @Test
  def test_function_of_scalar_with_takeright_0 {
    val ds = TakeRightOperation(0)(TestDataset.function_of_scalar)
    ds match {
      case Dataset(Function(s)) => assertEquals(true, s.isEmpty)
    }
  }
  
  @Test
  def test_function_of_scalar_with_takeright_1 {
    val ds = TakeRightOperation(1)(TestDataset.function_of_scalar)
    assertEquals(1, ds.getLength)
    ds match {
      case Dataset(Function(s)) => s.toList.head match {
        case Sample(Real(r1),Real(r2)) => {
            assertEquals(2.0,r1,0)
            assertEquals(2.0,r2,0)
        }
      }
    }
  }
  
  @Test
  def test_function_of_scalar_with_takeright_2 {
    val ds = TakeRightOperation(2)(TestDataset.function_of_scalar)
    assertEquals(2, ds.getLength)
    ds match {
      case Dataset(Function(s)) => s.toList.head match {
        case Sample(Real(r1),Real(r2)) => {
            assertEquals(1.0,r1,0)
            assertEquals(1.0,r2,0)
        }
      }
    }
  }
  
  @Test
  def test_function_of_scalar_with_takeright_5 {
    val ds = TakeRightOperation(5)(TestDataset.function_of_scalar)
    assertEquals(3, ds.getLength)
    ds match {
      case Dataset(Function(s)) => s.toList.head match {
        case Sample(Real(r1),Real(r2)) => {
            assertEquals(0.0,r1,0)
            assertEquals(0.0,r2,0)
        }
      }
    }
  }
  
  @Test
  def test_function_of_function_with_takeright_1 {
    val ds = TakeRightOperation(1)(TestDataset.function_of_functions)
    ds match {
      case Dataset(Function(f)) => f.toList.head match{
        case Sample(Integer(i1),Function(f1)) => { 
          assertEquals(3,i1)
          f1.toList.head match {
            case Sample(Integer(i2),Real(r2)) => {
              assertEquals(10,i2)
              assertEquals(30.0,r2,0)
            }
          }
        }
      }
    }    
  }
  
  @Test
  def test_empty_function {
    val ds = TakeRightOperation(5)(TestDataset.empty_function)
    ds match {
      case Dataset(x) => x match {
        case Function(it) => {
            assertEquals(true,it.isEmpty)
        }
      }
    }
  }
  
  @Test
  def test_metadata_length {
    val ds = TakeRightOperation(2)(TestDataset.function_of_scalar_with_length)
    assertEquals(Some("2"), ds.unwrap.asInstanceOf[Function].getMetadata("length"))
  }
  

  @Test
  def test_tsml_data {
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset
    val ds = TakeRightOperation(1)(data)
    ds match {
      case Dataset(x) => x match {
        case Function(f) => f.toList.head match {
          case Sample(Real(r1),Real(r2)) => {
            assertEquals(1628.5,r1,0)
            assertEquals(1360.4767,r2,0)
          }
        }
      }
    }
  }
  
  @Test
  def test_tsml_data_with_ops {
    val ops = ArrayBuffer[Operation]()
    ops += Operation("takeright",List("1"))
    val ds1 = TsmlReader("datasets/test/data_with_marker.tsml").getDataset
    val data = TsmlReader("datasets/test/data_with_marker.tsml").getDataset(ops)
    data match {
      case Dataset(x) => x match {
        case Function(f) => f.toList.head match {
          case Sample(Real(r1),Real(r2)) => {
            assertEquals(1628.5,r1,0)
            assertEquals(1360.4767,r2,0)
          }
        }
      }
    }
  }
    
  @Test
  def test_tuple_of_functions {
    assertEquals(TestDataset.tuple_of_functions, TakeRightOperation(1)(TestDataset.tuple_of_functions))
  }

}