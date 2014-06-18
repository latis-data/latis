package latis.reader

import org.junit.Test
import org.junit.Assert._
import java.io.FileOutputStream
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import scala.io.Source
import latis.dm.TestDataset

class TestBinaryAdapter {
  
  var tmpFile = java.io.File.createTempFile("LaTiS", "WriterTest")
  tmpFile.deleteOnExit
  
  @Test
  def test_reader{
    val fos = new FileOutputStream(tmpFile)
    val ds = TsmlReader("datasets/test/bin.tsml").getDataset
    Writer(fos,"txt").write(ds)
    fos.close()
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/tsi/txt").getLines
    while(t.hasNext) assertEquals(t.next, s.next) 
    assert(s.isEmpty)
  }
  
  //@Test
  def print_test{
    val ds = TsmlReader("datasets/test/bin.tsml").getDataset
    Writer.fromSuffix("asc").write(ds)
  }

}