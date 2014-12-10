package latis.reader

import java.io.FileOutputStream

import scala.io.Source

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.TsmlReader
import latis.writer.Writer

class TestJsonAdapter {
    
  var tmpFile = java.io.File.createTempFile("LaTiS", "TestJsonAdapter")
  tmpFile.deleteOnExit
  
  @Test
  def test_reader{
    val fos = new FileOutputStream(tmpFile)
    val ds = TsmlReader("datasets/test/json.tsml").getDataset
    Writer(fos,"txt").write(ds)
    fos.close()
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/dap2/txt").getLines
    while(t.hasNext) assertEquals(t.next, s.next) 
    assert(s.isEmpty)
  }
  
  @Test
  def catalog {
    val ds = TsmlReader("datasets/test/catalog.tsml").getDataset
    val data = ds.toStringMap
    val urls = data("accessURL")
    //for (url <- urls) println(url)
    assertEquals(6, urls.length)
    assertEquals("my_url_a1", urls.head)
  }
}
