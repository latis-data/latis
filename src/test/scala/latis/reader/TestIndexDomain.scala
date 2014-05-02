package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import latis.ops.filter._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter

class TestIndexDomain {
  
  @Test
  def test {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("index,myReal")
    val ds = TsmlReader("datasets/test/index.tsml").getDataset(ops)
    
    val data = ds.toDoubleMap
    
    assertEquals(2.0, data("index")(2), 0.0)
    
    //println(ds)
    //AsciiWriter.write(ds)
  }
  

}