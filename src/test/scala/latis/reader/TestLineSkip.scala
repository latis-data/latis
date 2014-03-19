package latis.reader

import latis.reader.tsml.ml._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestLineSkip  {
    
  @Test
  def granule_skip {
    val data = TsmlReader("datasets/test/granule_skip.tsml").getDataset.toStringMap
    assertEquals("A", data("myText")(0))
  }
  
  @Test
  def iterative_skip {
    val data = TsmlReader("datasets/test/iterative_skip.tsml").getDataset.toStringMap
    assertEquals("A", data("myText")(0))
  }
  

}