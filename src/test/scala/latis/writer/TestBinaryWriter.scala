package latis.writer

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.dm.TestDataset
import java.io.FileOutputStream

class TestBinaryWriter {
  
  var fos: FileOutputStream = null
  
  @Before
  def open_output_file {
    fos = new FileOutputStream("/data/test.bin")
  }
  
  @After
  def close_output_file {
    fos.close
  }
  
  //@Test
  def real = Writer(fos, "bin").write(TestDataset.real)
  
  def integer = Writer(fos, "bin").write(TestDataset.integer)
  def text = Writer(fos, "bin").write(TestDataset.text)
  
  @Test
  def test = Writer(fos, "bin").write(TestDataset.index_function)
  
  //@Test 
  def empty_dataset = Writer(fos, "bin").write(TestDataset.empty)
  
  //@Test
  def empty_function = Writer(fos, "bin").write(TestDataset.empty_function)
}