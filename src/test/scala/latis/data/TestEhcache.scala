package latis.data

import java.io.FileOutputStream

import scala.io.Source

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.TsmlReader
import latis.writer.Writer

class TestEhcache {
  
  //@Test
  def write_twice {
    var tmp1 = java.io.File.createTempFile("LaTiS", "CacheTest1")
    tmp1.deleteOnExit
    val fos1 = new FileOutputStream(tmp1)
    var tmp2 = java.io.File.createTempFile("LaTiS", "CacheTest2")
    tmp2.deleteOnExit
    val fos2 = new FileOutputStream(tmp2)
    
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    
    Writer(fos1,"asc").write(ds)
    fos1.close()
    Writer(fos2,"asc").write(ds)
    fos2.close()
    
    val s1 = Source.fromFile(tmp1, "ISO-8859-1").getLines
    val s2 = Source.fromFile(tmp2, "ISO-8859-1").getLines
    while(s1.hasNext) assertEquals(s1.next, s2.next)
    assert(s2.isEmpty)
  }
  
  @Test
  def it_twice {
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val f = ds.findFunction.get
    val it = f.iterator
    f.iterator.toList.foreach(_.equals(it.next))
  }
  
}