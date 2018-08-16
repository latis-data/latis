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
  def contains_valid_extant_time = {
    val ops = ArrayBuffer[Operation]()
    ops += new Contains("myTime", Seq("1970-01-02", "1970-01-03")) //TODO: reformat without "new" after companion object exists
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
  def contains_valid_extant_integers = {
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("B", Seq("11", "12", "13")) //TODO: reformat without "new" after companion object exists
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
  def contains_valid_extant_reals = {
    val ops = ArrayBuffer[Operation]()
    ops += new Contains("myReal", Seq("3.3", "1.1")) //TODO: reformat without "new" after companion object exists
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
  def contains_valid_extant_text = {
    val ops = ArrayBuffer[Operation]()
    ops += new Contains("myText", Seq("B", "C")) //TODO: reformat without "new" after companion object exists
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
    ops += new Contains("myTime", Seq("2018-05-01", "2018-05-02", "2018-05-04T00:00:00.000Z")) 
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)   
    }
  }
  
  @Test
  def contains_valid_nonexistent_integers = {
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("C", Seq("110", "120", "130")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test 
  def contains_valid_nonexistent_reals = {
    val ops = ArrayBuffer[Operation]()
    ops += new Contains("myReal", Seq("100.01", "2000.002")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test 
  def contains_valid_nonexistent_text = {
    val ops = ArrayBuffer[Operation]()
    ops += new Contains("myText", Seq("X", "Y", "Z")) 
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test 
  def multiple_contains_ops = {
    val ops = ArrayBuffer[Operation]()
    ops += new Contains("myReal", Seq("3.3", "1.1")) //TODO: reformat without "new" after companion object exists
    ops += new Contains("myText", Seq("C"))          //TODO: reformat without "new" after companion object exists
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
    val op = new Contains("myTime", Seq("1970/01", "1970/03")) 
    val ds2 = op(ds)
    
    ds2 match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_invalid_integer = {
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("B", Seq("11", "invalid", "13")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    println("LOOK!")
    latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, TupleMatch(Integer(b), Integer(c))) => {
          assertEquals(11, b)
        }
        it.next match { //TODO: as Contains is currently written, an exception stops this sample from making it
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
    ops += new Contains("myReal", Seq("1.1", "invalid", "3.3")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    println("LOOK!")
    latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, Tuple(vars)) => vars match { 
          case Vector(Integer(i), Real(r), Text(t)) => {
            assertEquals(1.1, r, 0)
          }
        }
        it.next match { //TODO: as Contains is currently written, an exception stops this sample from making it
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
    ops += new Contains("myText", Seq("1", "", "C")) //TODO: reformat without "new" after companion object exists
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
    ops += new Contains("B", Seq("x", "y", "z")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_invalid_nonexistent_reals = { 
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("myReal", Seq("x", "one", "z")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_invalid_nonexistent_text = { //TODO: is this a dumb test? Maybe just a dumb name?
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("myText", Seq("1", "2.2", "")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("dap2").getDataset(ops)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  
  
}




