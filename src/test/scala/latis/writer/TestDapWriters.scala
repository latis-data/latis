package latis.writer

import latis.dm._
import java.io.FileOutputStream
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import latis.reader.tsml._
import latis.reader.tsml.ml._
import latis.ops.Projection
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.ops.Selection
import latis.ops.LastFilter
import latis.ops.FirstFilter
import latis.ops.LimitFilter
import scala.io.Source

import org.junit._
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import latis.dm._

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
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/$name/$suffix").getLines
    assertEquals(t.length,s.length)
    while(t.hasNext) assertEquals(t.next, s.next)
    fos.close()
  }
  
  //@Test
  def write_dds {
    val reader = TsmlReader("datasets/test/tsi.tsml")
    val ds = reader.getDataset
    Writer("dds").write(ds)
  }
    
  //@Test
  def write_das {
    val reader = TsmlReader("datasets/test/tsi.tsml")
    val ds = reader.getDataset
    Writer("das").write(ds)
  }
  
}