package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.dm.TestDataset
import latis.writer.AsciiWriter

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
  
  @Test
  def nested {
    val ds = TextAppender("x", "x")(TextAppender("y", "y")(TextAppender("z", "z")(TestDataset.function_of_functions_text)))
    val data = ds.toStrings
    assertEquals("0x", data(0)(0))
    assertEquals("12y", data(1)(8))
    assertEquals("11z", data(2)(4))
  }

}