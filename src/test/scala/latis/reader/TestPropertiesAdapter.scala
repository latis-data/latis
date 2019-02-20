package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter

class TestPropertiesAdapter {
  
  @Test
  def test = {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("dataset.dir")
    val ds = TsmlReader("datasets/test/properties.tsml").getDataset(ops)
    
    assertEquals("datasets/test", ds.toStringMap("dataset.dir").head)
    
    //AsciiWriter.write(ds)
  }
}