package latis.writer

import java.io.FileOutputStream
import org.junit._
import Assert._
import scala.io.Source
import latis.reader.tsml.TsmlReader
import java.io.DataOutputStream
import java.io.File

class TestDapWriters {

  var tmpFile = java.io.File.createTempFile("writer", "test")
  tmpFile.deleteOnExit
    
  val names = List("scalar", "tsi","dap2")
    
  //@Test
  def test_all {
    for(name <- names) {
      test(name,"dds")
      test(name,"das")
      test(name,"dods")
    }
  }
  
  def test(name: String, suffix: String) {
    val fos = new FileOutputStream(tmpFile)
    val ds = TsmlReader(s"datasets/test/$name.tsml").getDataset
    Writer(fos,suffix).write(ds)
    fos.close()
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/$name/$suffix").getLines
    while(t.hasNext) assertEquals(t.next, s.next)
    assert(s.isEmpty)
  }
  
  @Test
  def write_dds {
    val reader = TsmlReader("datasets/test/composite_mg_index.tsml")
    val ds = reader.getDataset()
    Writer.fromSuffix("csv").write(ds)
  }
    
  //@Test
  def write_das {
    val reader = TsmlReader("datasets/test/dap2.tsml")
    val ds = reader.getDataset
    Writer.fromSuffix("das").write(ds)
  }
  
  //@Test 
  def write_dods {
    val fos = new DataOutputStream(new FileOutputStream(new File("src/test/resources/datasets/data/dap2/dods")))
    val ds = TsmlReader("datasets/test/dap2.tsml").getDataset
    Writer(fos,"dods").write(ds)
    fos.close()
  }
}
