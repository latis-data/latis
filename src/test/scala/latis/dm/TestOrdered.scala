package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.LazyLogging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter
import latis.writer.Writer
import latis.data.Data
import latis.data.SampledData
import latis.data.seq.DataSeq

class TestOdered {
  
  @Test
  def inequality = {
    val b = Real(3.14) > Integer(3)
    assertTrue(b)
  }
}