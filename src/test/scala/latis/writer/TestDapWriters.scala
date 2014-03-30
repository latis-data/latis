package latis.writer

import java.io.FileOutputStream
import org.junit._
import Assert._
import scala.io.Source
import latis.reader.tsml.TsmlReader

class TestDapWriters {

  var tmpFile = java.io.File.createTempFile("writer", "test")
  tmpFile.deleteOnExit
    
  val names = List("scalar","tsi","db")
    
  @Test
  def test_all {
    for(name <- names) {
      test(name,"dds")
      test(name,"das")
    }
  }
  
  def test(name: String, suffix: String) {
    val fos = new FileOutputStream(tmpFile)
    val ds = TsmlReader(s"datasets/test/$name.tsml").getDataset
    Writer(fos,suffix).write(ds)
    fos.close()
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/$name/$suffix").getLines
    assertEquals(t.length,s.length)
    while(t.hasNext) assertEquals(t.next, s.next)
  }
  
  //@Test
  def write_dds {
    val reader = TsmlReader("datasets/test/db.tsml")
    val ds = reader.getDataset
    Writer.fromSuffix("dds").write(ds)
  }
    
  //@Test
  def write_das {
    val reader = TsmlReader("datasets/test/db.tsml")
    val ds = reader.getDataset
    Writer.fromSuffix("das").write(ds)
  }
  
}