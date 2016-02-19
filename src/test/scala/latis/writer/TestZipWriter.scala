package latis.writer

import org.junit.Test
import latis.reader.tsml.TsmlReader

class TestZipWriter {
  
  @Test(expected=classOf[IllegalArgumentException])
  def non_file_list {
    val w = Writer("foo.zip")
    val ds = TsmlReader("log/log_join.tsml").getDataset
    w.write(ds)
  }
  
  //@Test //can't really be automated
  def file_list {
    val w = Writer("test.zip")
    val ds = TsmlReader("log/log_list.tsml").getDataset
    w.write(ds)
  }
  
}