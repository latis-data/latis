package latis.reader

import org.junit.Assert._
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
  def test_adapter_array {
    val ds = TsmlReader("datasets/test/json_array.tsml").getDataset
    val data = ds.toStrings
    assertEquals(5, data.length)
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
  
  @Test
  def test_reader_array {
    val ds = JsonReader("datasets/test/mixed_array.json").getDataset
    val data = ds.toStrings
    assertEquals(4, data.length)
    assertEquals(3, data(2).length)
    assertEquals("2", data(1)(1))
  }
  
}
