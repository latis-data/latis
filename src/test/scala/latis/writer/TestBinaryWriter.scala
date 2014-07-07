package latis.writer

import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.dm.TestDataset
import java.io.FileOutputStream

class TestBinaryWriter extends WriterTest{
  
  var fos: FileOutputStream = null
    
  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"bin")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"bin")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"bin")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"bin")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"bin")
  }
  
  //@Test 
  def write_bin_file {
    write_to_file(fof, "bin")
  }
  
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
  
  //@Test
  def testb = Writer(fos, "bin").write(TestDataset.index_function)
  
  //@Test 
  def empty_dataset = Writer(fos, "bin").write(TestDataset.empty)
  
  //@Test
  def empty_function = Writer(fos, "bin").write(TestDataset.empty_function)
}