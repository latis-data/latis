package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.TsmlReader

class TestJsonAdapter {
  
  @Test
  def test_adapter {
    val ds = TsmlReader("datasets/test/json.tsml").getDataset
    val data = ds.toStrings
    assertEquals(4, data.length)
    assertEquals(3, data(2).length)
    assertEquals("2", data(1)(1))
  }
  
  @Test
  def test_reader {
    val ds = JsonReader("datasets/test/mixed.json").getDataset
    val data = ds.toStrings
    assertEquals(4, data.length)
    assertEquals(3, data(2).length)
    assertEquals("2", data(1)(1))
  }
  
}
