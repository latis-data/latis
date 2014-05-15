package latis.reader

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.reader.tsml.TsmlReader
import latis.ops.Operation
import latis.writer.AsciiWriter
import latis.ops.filter.Selection

class TestCalendarAdapter {
  
  @Test
  def ical {
    val ops = ArrayBuffer[Operation]()
    ops += Selection("time >= 2014-05-01")
    ops += Selection("time <  2014-06-01")
    val ds = TsmlReader("work_cal.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
    
}