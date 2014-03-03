package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.ops.Selection
import java.io.File
import latis.util.FileUtils

class TestFileListAdapter extends AdapterTests {

  def datasetName = "files"
  
  //@Test
  def test = writeDataset
}

object TestFileListAdapter {
  
  val tmpDir: File = {
    //val dir = new File(System.getProperty("java.io.tmpdir") + File.separator + "latis_file_test")
    //TODO: java.io.tmpdir ends with "/" on Mac, off by one when removing dir in FileUtils.list
    //  /var/folders/Jl/JlPXu2FoEs0GXqwaPMPc2++++TY/-Tmp-//latis_file_test
    val dir = new File("/tmp/latis_file_test")
    dir.mkdir
    dir
  }
  
  @BeforeClass
  def makeTmpFiles {
    //make sure this remains consistent with shared AdapterTests
    (new File(tmpDir, "Foo1970001bar1v1.1A.dat")).createNewFile
    (new File(tmpDir, "Foo1970002bar2v2.2B.dat")).createNewFile
    (new File(tmpDir, "Foo1970003bar3v3.3C.dat")).createNewFile
    //test with non-matching file
    (new File(tmpDir, "junk")).createNewFile
  }
  
  @AfterClass
  def removeTmpFiles {
    FileUtils.delete(tmpDir)
  }
}