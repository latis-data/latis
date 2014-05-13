package latis.writer

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import java.io.File
import java.io.FileOutputStream

class TestImageWriter extends WriterTest {
  
  override val names = List("scalar", "dap2", "tsi")
  
  //@Test
  def test_png {
    for(name <- names) test_writer(getDataset(name),"png")
  }
  
  //@Test
  def writeToTestFile{
    val file = new File("/tmp/latis_image_writer_test.png")
    val ds = getDataset("historical_tsi")
    Writer(file.toString).write(ds)
  }

  //@Test 
  def write_image_file {
    for(name <- names)
    write_to_file(name, "png")
  }

}