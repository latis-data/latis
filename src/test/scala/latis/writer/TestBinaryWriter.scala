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
  
  @Test
  def real = {
    val fos = new FileOutputStream("/data/test.bin")
    Writer(fos, "bin").write(TestDataset.real)
    fos.close
  }
  
  def integer = Writer("bin").write(TestDataset.integer)
  def text = Writer("bin").write(TestDataset.text)
  
  //@Test
  def test = Writer("bin").write(TestDataset.index_function)
  
  //@Test 
  def empty_dataset = Writer("bin").write(TestDataset.empty)
  
  //@Test
  def empty_function = Writer("bin").write(TestDataset.empty_function)
}