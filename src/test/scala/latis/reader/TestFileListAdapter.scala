package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.ops.Selection
import java.io.File

class TestFileListAdapter { //extends AdapterTests {

  def datasetName = "files"
  
  @Test
  def test {
    //val ops = List(Selection("time>2000-01-01"))
    val ds = TsmlReader("datasets/test/files.tsml").getDataset
    AsciiWriter().write(ds)
  }
}

object TestFileListAdapter {
  
  val tmpDir = {
    //Must hard-code tmp dir so tsml can point to it. 
    //TODO: parameterize in properties?
    val dir = new File(System.getProperty("java.io.tmpdir") + File.separator + "latis_file_test")
    //val dir = new File(File.separator + "tmp" + File.separator + "latis_file_test")
    dir.mkdir
    dir.deleteOnExit //make sure we don't leave it around
    dir
  }
  
  @BeforeClass
  def makeTmpFiles {
    //make sure this remains consistent with shared AdapterTests
    (new File(tmpDir, "Foo1970001bar1v1.1A.dat")).createNewFile
    (new File(tmpDir, "Foo1970002bar2v2.2B.dat")).createNewFile
    (new File(tmpDir, "Foo1970003bar3v3.3C.dat")).createNewFile
  }
  
  @AfterClass
  def removeTmpFiles {
    tmpDir.delete //redundant with deleteOnExit, but oh well
  }
}