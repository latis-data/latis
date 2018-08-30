package latis.ops

import scala.collection.mutable.ArrayBuffer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.TestDataset
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.TupleMatch
import latis.ops.filter.Contains
import latis.reader.DatasetAccessor

class TestContains {
  
  @Test 
  def contains_valid_existent_time = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myTime={1970-01-02, 1970-01-03}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Text(t), _) => { 
          assertEquals("1970/01/02", t)
        }
        it.next match {
          case Sample(Text(t), _) => {
            assertEquals("1970/01/03", t)
          }
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test
  def contains_valid_existent_integers = {
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("B", Seq("11", "12", "13")) 
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, TupleMatch(Integer(b), Integer(c))) => {
          assertEquals(11, b)
        }
        it.next match {
          case Sample(_, TupleMatch(Integer(b), Integer(c))) => {
            assertEquals(12, b)
          }
        }
        it.next match {
          case Sample(_, TupleMatch(Integer(b), Integer(c))) => {
            assertEquals(13, b)
          }
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test 
  def contains_valid_existent_reals = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myReal = {3.3,1.1}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Tuple(vars)) => vars match { 
          case Vector(Integer(i), Real(r), Text(t)) => {  
            assertEquals(1, i)
            assertEquals(1.1, r, 0)
            assertEquals("A", t)
          }
        }
        it.next match {
          case Sample(_, Tuple(vars)) => vars match { 
            case Vector(Integer(i), Real(r), Text(t)) => { 
              assertEquals(3, i) 
              assertEquals(3.3, r, 0)
              assertEquals("C", t)
            }
          }
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test 
  def contains_valid_existent_text = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myText={B,C}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Tuple(vars)) => vars match { 
          case Vector(Integer(i), Real(r), Text(t)) => assertEquals("B", t)
        }
        it.next match {
          case Sample(_, Tuple(vars)) => vars match {
            case Vector(Integer(i), Real(r), Text(t)) => assertEquals("C", t)
          }
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test 
  def contains_valid_nonexistent_time = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myTime = { 2018-05-01, 2018-05-02, 2018-05-04T00:00:00.000Z }") 
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)   
    }
  }
  
  @Test
  def contains_valid_nonexistent_integers = {
    val ops = ArrayBuffer[Operation]() 
    ops += Contains("C={110, 120, 130}")
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test 
  def contains_valid_nonexistent_reals = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myReal={100.01, 2000.002}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test 
  def contains_valid_nonexistent_text = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myText={X, Y, Z}") 
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test 
  def multiple_contains_ops = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myReal={3.3, 1.1}") 
    ops += Contains("myText={C}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Tuple(vars)) => vars match { 
          case Vector(Integer(i), Real(r), Text(t)) => {  
            assertEquals(3, i)
            assertEquals(3.3, r, 0)
            assertEquals("C", t)
          }
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test
  def contains_invalid_time = {
    val ds = TestDataset.time_series
    val op = Contains("myTime={1970/01, 1970/03}") 
    val ds2 = op(ds)
    
    ds2 match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_invalid_integer = {
    val ops = ArrayBuffer[Operation]() 
    ops += Contains("B  =  {11, invalid,13}")
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, TupleMatch(Integer(b), Integer(c))) => {
          assertEquals(11, b)
        }
        it.next match { 
          case Sample(_, TupleMatch(Integer(b), Integer(c))) => {
            assertEquals(13, b)
          }
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test
  def contains_invalid_real = {
    val ops = ArrayBuffer[Operation]() 
    ops += Contains("myReal={1.1, invalid, 3.3}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Tuple(vars)) => vars match { 
          case Vector(Integer(i), Real(r), Text(t)) => {
            assertEquals(1.1, r, 0)
          }
        }
        it.next match { 
          case Sample(_, Tuple(vars)) => vars match { 
            case Vector(Integer(i), Real(r), Text(t)) => {
              assertEquals(3.3, r, 0)
            }
          }
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test 
  def contains_invalid_text = {
    val ops = ArrayBuffer[Operation]()
    ops += Contains("myText={1, , C}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Tuple(vars)) => vars match { 
          case Vector(Integer(i), Real(r), Text(t)) => assertEquals("C", t)
        }
        assertFalse(it.hasNext)
      }
    }
  }
  
  @Test
  def contains_invalid_nonexistent_integers = { 
    val ops = ArrayBuffer[Operation]() 
    ops += Contains("B={x, y, z}")
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_invalid_nonexistent_reals = { 
    val ops = ArrayBuffer[Operation]() 
    ops += Contains("myReal={x, one, z}")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_invalid_nonexistent_text = { 
    val ops = ArrayBuffer[Operation]() 
    ops += Contains("myText={1, 2.2, }")
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def unapply_contains = {
    val contains = Contains("a={1, 2.2, three}")
    contains match {
      case Contains(vname, values) => {
        assertEquals("a", vname)
        assertEquals(Seq("1", "2.2", "three"), values)
      }
    }
  }
  
}
