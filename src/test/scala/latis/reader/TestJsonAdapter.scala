package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import latis.ops.filter._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter

class TestJsonAdapter {
  
  @Test
  def test_catalog {
    val ops = ArrayBuffer[Operation]()
    val ds = TsmlReader("datasets/test/catalog.tsml").getDataset(ops)
    
    
    
    //AsciiWriter.write(ds)
  }
}