package latis.data

import java.io.FileOutputStream

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.io.Source

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.TestDataset
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
  
//  @Test
  def reuse_iterator {
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val f = ds match {
      case Dataset(v) => v.asInstanceOf[Function]
      case _ => null
    }
    assertEquals(10, f.iterator.length)
    assertEquals(10, f.iterator.length)
    assertEquals(10, f.iterator.length)
    
  }
  
//  @Test 
  def two_functions { //caching doesn't work when alternating functions
    val ds1 = TestDataset.function_of_scalar
    val ds2 = TestDataset.function_of_tuple
    val (f1, f2) = (ds1, ds2) match {
      case (Dataset(v1), Dataset(v2)) => (v1.asInstanceOf[Function], v2.asInstanceOf[Function])
      case _ => (null, null)
    }
    
    assertEquals(3, f1.iterator.length)
    assertEquals(3, f2.iterator.length)
    assertEquals(0, f1.iterator.length)
  }
  
//  @Test
  def nested_function { //caching doesn't work with nested functions
    val ds = TestDataset.function_of_functions
    val f = ds match {
      case Dataset(v) => v.asInstanceOf[Function]
      case _ => null
    }
    assertEquals(4, f.iterator.length)
    assertEquals(0, f.iterator.length)
  }
  
}
