package latis.writer

import latis.dm.implicits._
import org.junit._
import Assert._

class WriterTest {
  
  @Test
  def test {
    val w = Writer(System.out, "csv")
    w.write(1.0)
  }
  
}