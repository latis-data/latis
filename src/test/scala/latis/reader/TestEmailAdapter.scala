package latis.reader

import org.junit.Assert._
import org.junit.Test
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.writer.JsonWriter
import latis.writer.Writer
import latis.ops.filter.Selection
import org.junit.Ignore
import latis.ops.Projection
import latis.ops.Operation
import latis.ops.TimeFormatter

class TestEmailAdapter {

  //need to include password in tsml
  
  //@Test 
  def gmail_folder {
    val ops = List(Selection("sender=~.*James.*"))
    val ds = TsmlReader("datasets/test/agu_email.tsml").getDataset(ops)
    //Writer.fromSuffix("json").write(ds)
    val data = ds.toStringMap
    assertEquals(3, data("sender").length)
  }  
  
  //@Test
  def lasp_folder {
    //TODO: selection having no effect with projection
    //val ops = List(Projection("subject"), Selection("time>2015-01-01"))
    val ops = List(Selection("time>2015-01-01"))
    val ds = TsmlReader("datasets/test/email.tsml").getDataset(ops)
    val data = ds.toStringMap
    assert(data("subject").head.startsWith("Your Free Ebook"))
  }
  
  //@Test
  def nested_folder {
    //Note, time zone is consistent with these times being UTC.
    val ops = List(Projection("time,subject"), Selection("time>2015-05-05"), Selection("time<2015-05-05T16"), TimeFormatter("yyyy-MM-dd HH:mm:ss"))
    val ds = TsmlReader("datasets/test/nested_email.tsml").getDataset(ops)
    //Writer.fromSuffix("csv").write(ds)
    val data = ds.toStringMap
    assertEquals(1, data("time").length)
    assertEquals("2015-05-05 15:48:55", data("time").head)
  }
}