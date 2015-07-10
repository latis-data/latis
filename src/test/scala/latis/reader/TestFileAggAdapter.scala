package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter
import latis.reader.tsml.TsmlReader

class TestFileAggAdapter {
  
  @Test
  def first {
    val ops = List(FirstFilter())
    val ds = TsmlReader("datasets/test/agg/file_agg.tsml").getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(0.0, data("T").head, 0.0)
  }
  
  @Test
  def last {
    val ops = List(LastFilter())
    val ds = TsmlReader("datasets/test/agg/file_agg.tsml").getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(19.0, data("T").head, 0.0)
  }

}