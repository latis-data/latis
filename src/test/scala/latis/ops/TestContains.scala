package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Assert._
import org.junit.Test
import latis.data.SampledData
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.TestDataset
import latis.dm.implicits.variableToDataset
import latis.metadata.Metadata
import latis.ops.filter.NearestNeighborFilter
import latis.ops.filter.Selection
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.reader.DatasetAccessor
import scala.collection.mutable.ArrayBuffer
import latis.ops.filter.Contains
import latis.dm.Sample
import latis.dm.TupleMatch
import latis.dm.Text
import latis.dm.Integer

//TODO: Organize imports
class TestContains {
  
  // time -> value
  lazy val scalarFunction: Function = {
    val domain = Real(Metadata("time"))
    val range = Real(Metadata("value"))
    val data = SampledData.fromValues(Seq(1,2,3,4,5), Seq(1,2,3,4,5))
    Function(domain, range, data=data)
  }
  
  @Test
  def contains_valid_extant_values = {
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
  def contains_valid_nonexistent_values = {
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("B", Seq("110", "120", "130")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_invalid_nonexistent_values = {
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("B", Seq("x", "y", "z")) //TODO: reformat without "new" after companion object exists
    val ds = DatasetAccessor.fromName("agg/scalar_ts_3col_10to19").getDataset(ops)
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(Function(it)) => assertFalse(it.hasNext)
    }
  }
  
  @Test
  def contains_some_invalid_values = {
    val ops = ArrayBuffer[Operation]() 
    ops += new Contains("B", Seq("11", "invalid", "13")) //TODO: reformat without "new" after companion object exists
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
  
  
  
}




