package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.TsmlReader

class TestFileListGenerator {
  
  @Test
  def format {
    val ds = TsmlReader("file_gen.tsml").getDataset
    val data = ds.toStringMap
    assertEquals("1907", data("time").head)
    assertEquals("1923/23.dat", data("file").last)
    
  }

}