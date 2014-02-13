package latis.writer

import latis.dm._
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

  var tmpFile = ???
  
  def getTmpDir = System.getProperty("java.io.tmpdir")
  
  @After
  def removeTmpFile = ???
  
  @Test
  def test_dds {
    test_scalar_dds
    test_db_dds
    test_tsi_dds
  }
  @Test
  def test_das {
    test_scalar_das
    test_db_das
    test_tsi_das
  }
  
  @Test
  def write_dds {
    val reader = TsmlReader("datasets/tsi.tsml")
    val ds = reader.getDataset
    Writer("dds").write(ds)
  }
    
  @Test
  def write_das {
    val reader = TsmlReader("datasets/tsi.tsml")
    val ds = reader.getDataset
    Writer("das").write(ds)
  }
  
  def test_scalar_dds {
    val reader = TsmlReader("datasets/scalar.tsml")
    val ds = reader.getDataset
    val w = new DdsWriter()
    val s = (w.makeHeader(ds) + ds.getVariables.map(w.varToString(_)).mkString("") + w.makeFooter(ds)).split("\n")
    val t = Source.fromFile("src/test/resources/datasets/data/scalar/dds").getLines.toArray[String]
    assertEquals(t.length,s.length)
    for(a <- 0 until t.length) assertEquals(t(a), s(a))
  }
  
  def test_db_dds {
    val reader = TsmlReader("datasets/test/db.tsml")
    val ds = reader.getDataset
    val w = new DdsWriter()
    val s = (w.makeHeader(ds) + ds.getVariables.map(w.varToString(_)).mkString("") + w.makeFooter(ds)).split("\n")
    val t = Source.fromFile("src/test/resources/datasets/data/db/dds").getLines.toArray[String]
    assertEquals(t.length,s.length)
    for(a <- 0 until t.length) assertEquals(t(a), s(a))
  }
  
  def test_tsi_dds {
    val reader = TsmlReader("datasets/tsi.tsml")
    val ds = reader.getDataset
    val w = new DdsWriter()
    val s = (w.makeHeader(ds) + ds.getVariables.map(w.varToString(_)).mkString("") + w.makeFooter(ds)).split("\n")
    val t = Source.fromFile("src/test/resources/datasets/data/tsi/dds").getLines.toArray[String]
    assertEquals(t.length,s.length)
    for(a <- 0 until t.length) assertEquals(t(a), s(a))
  }
  
  def test_scalar_das {
    val reader = TsmlReader("datasets/scalar.tsml")
    val ds = reader.getDataset
    val w = new DasWriter()
    val s = (w.makeHeader(ds) + ds.getVariables.map(w.varToString(_)).mkString("") + w.makeFooter(ds)).split("\n")
    val t = Source.fromFile("src/test/resources/datasets/data/scalar/das").getLines.toArray[String]
    assertEquals(t.length,s.length)
    for(a <- 0 until t.length) assertEquals(t(a), s(a))
  }
  
  def test_db_das {
    val reader = TsmlReader("datasets/test/db.tsml")
    val ds = reader.getDataset
    val w = new DasWriter()
    val s = (w.makeHeader(ds) + ds.getVariables.map(w.varToString(_)).mkString("") + w.makeFooter(ds)).split("\n")
    val t = Source.fromFile("src/test/resources/datasets/data/db/das").getLines.toArray[String]
    assertEquals(t.length,s.length)
    for(a <- 0 until t.length) assertEquals(t(a), s(a))
  }
  
  def test_tsi_das {
    val reader = TsmlReader("datasets/tsi.tsml")
    val ds = reader.getDataset
    val w = new DasWriter()
    val s = (w.makeHeader(ds) + ds.getVariables.map(w.varToString(_)).mkString("") + w.makeFooter(ds)).split("\n")
    val t = Source.fromFile("src/test/resources/datasets/data/tsi/das").getLines.toArray[String]
    assertEquals(t.length,s.length)
    for(a <- 0 until t.length) assertEquals(t(a), s(a))
  }
}