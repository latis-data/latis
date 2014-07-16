package latis.writer

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import java.io.File
import java.io.FileOutputStream

class TestImageWriter extends WriterTest {
  
  //@Test
  def test_tsi {
    test_writer(getDataset("tsi"),"png")
  }
  
  //@Test
  def writeToTestFile{
    val file = new File("/tmp/latis_image_writer_test.png")
    val ds = getDataset("historical_tsi")
    Writer(file.toString).write(ds)
  }

  //@Test 
  def write_image_file {
    write_to_file("tsi", "png")
  }

}