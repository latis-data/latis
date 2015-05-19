package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.TsmlReader

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

}