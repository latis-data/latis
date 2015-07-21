package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.ops.filter.Selection

class TestFileAggAdapter {
  
  @Test
  def first {
    val ops = List(FirstFilter())
    val ds = TsmlReader("agg/file_agg.tsml").getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(0.0, data("T").head, 0.0)
  }
  
  @Test
  def last {
    val ops = List(LastFilter())
    val ds = TsmlReader("agg/file_agg.tsml").getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(19.0, data("T").head, 0.0)
  }
  
  @Test
  def log {
    val ops = ArrayBuffer[Operation]()
    ops += Selection("time>2015-07-10")
    val ds = TsmlReader("log/log_agg.tsml").getDataset(ops)
    val data = ds.toStringMap
    assertEquals(3, data("message").length)
    }
  
  @Test
  def list {
    val ops = ArrayBuffer[Operation]()
    ops += Selection("time>2015-07-10")
    val ds = TsmlReader("log/log_list.tsml").getDataset(ops)
    val data = ds.toStringMap
    assertEquals(2, data("file").length)
  }
  
}