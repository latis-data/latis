package latis.writer

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging

class WriterTest extends Logging {
  
  @Test
  def test {
    val w = Writer(System.out, "csv")
    logger.info("Hodwy")
    w.write(1.0)
  }
  
}