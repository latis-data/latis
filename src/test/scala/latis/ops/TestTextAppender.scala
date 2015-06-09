package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.dm.TestDataset

class TestTextAppender {
  
  @Test
  def data {
    val ds = TextAppender("myText", "test")(TestDataset.canonical)
    val data = ds.toStrings
    assertEquals("Btest", data(3)(1))
    assertEquals("1970/01/03", data(0)(2))
  }
  
  @Test
  def metadata {
    val ds = TextAppender("myText", "test")(TestDataset.canonical)
    assertEquals("5", ds.findVariableByName("myText").get.getMetadata("length").get)
  }

}