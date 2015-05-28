package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.ops.filter.Selection
import latis.ops.Projection

class TestLogStatsAdapter {
  
  @Test
  def test {
    val ds = TsmlReader("datasets/test/log_stats_test.tsml").getDataset
    val data = ds.toStrings
    assertEquals(4, data.length)
    assertEquals(3, data(1).length)
    assertEquals("1970-01-01", data(0)(0))
    assertEquals(2, data(1)(1).toDouble.toInt)
    assertEquals(3.3, data(2)(2).toDouble, 0)
    assertEquals("A", data(3)(0))
  }
  
  @Test
  def time_selection_split_off_request {
    val ops = List(Selection("time>1970-01-02"))
    val ds = TsmlReader("datasets/test/log_stats_test.tsml").getDataset(ops)
    val data = ds.toStrings
    assertEquals(1, data(0).length)
  }
  
  /*
   * This currently fails because it filters after the stats are generated.
   */
  @Test
  def time_selection_split_off_response {
    val ops = List(Selection("time<=1970-01-02"))
    val ds = TsmlReader("datasets/test/log_stats_test.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toStrings
    assertEquals(1, data(0).length)
  }
  
  @Test
  def projection {
    val ops = List(Projection("time,myInt"))
    val ds = TsmlReader("datasets/test/log_stats_test.tsml").getDataset(ops)
    val data = ds.toStrings
    assertEquals(2, data.length)
  }

  //@Test //stack overflow
  def live {
    val ops = List(Selection("time>2015-05-28"))
    val ds = TsmlReader("datasets/test/log_stats.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
}