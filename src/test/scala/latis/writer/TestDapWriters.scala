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
  
  /*
   * For lack of a better solution at this time, dods must be tested manually.
   * The saved file can be compared to the output of pydap using the following commands in python:
   * 	>>> from pydap.client import open_dods
   *    >>> dods = open_dods('http://localhost:8080/LaTiS/dap/test/ **name** .dods')
   *    >>> len(dods.samples.data)
   *    	This should be equal to the number of lines in the saved file
   *    >>> print dods.samples.data
   *    	This will print the data values which can be compared to the saved file
   */
  
  //@Test
  def write_dds {
    val reader = TsmlReader("datasets/test/db.tsml")
    val ds = reader.getDataset
    Writer("dds").write(ds)
  }
    
  //@Test
  def write_das {
    val reader = TsmlReader("datasets/test/db.tsml")
    val ds = reader.getDataset
    Writer("das").write(ds)
  }
  
}