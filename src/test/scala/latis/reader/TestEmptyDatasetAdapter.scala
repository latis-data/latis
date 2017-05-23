package latis.reader

import org.junit.Assert._
import org.junit.Test
import latis.dm._
import latis.ops.Operation
import latis.ops.filter.Selection
import latis.ops.Projection
import latis.time.Time

class TestEmptyDatasetAdapter {
  
  @Test
  def empty = {
    val ds = DatasetAccessor.fromName("empty").getDataset
    assertEquals(0, ds.getLength)
  }
  
  @Test
  def type_preserved = {
    val ds = DatasetAccessor.fromName("empty").getDataset
    ds match {
      case Dataset(f: Function) => {
        f.getDomain match {
          case _: Time => 
          case _ => fail    
        }
        f.getRange match {
          case TupleMatch(_: Integer, _: Real, _: Text) =>
          case _ => fail
        }
      }
    }
  }
  
  @Test
  def empty_with_ops = {
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += Selection("myInt=0")
    ops += Projection("myReal")
    val ds = DatasetAccessor.fromName("empty").getDataset(ops)
    latis.writer.Writer.fromSuffix("asc").write(ds)
    ds match {
      case Dataset(f: Function) => {
        assertTrue(f.getDomain.isInstanceOf[Index])
        assertTrue(f.getRange.isInstanceOf[Real])
      }
    }
  }
  
  @Test
  def preserve_metadata = {
    val ds = DatasetAccessor.fromName("empty").getDataset
    ds match {
      case Dataset(f: Function) => {
        f.getDomain.getMetadata("units") match {
          case Some(u) => assertEquals("yyyy/MM/dd", u)
          case None => fail
        }
      }
    }
  }
}