package latis.writer

import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.dm.TestDataset
import java.io.FileOutputStream
import java.nio.ByteBuffer

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
  def print_bytes {
    val w = new BinaryWriter
    val ds = getDataset(fof)
    val v = ds.unwrap
    for(b <- w.varToBytes(v)) println(b.toInt.toHexString.replace("ffffff", ""))
    
    AsciiWriter.write(getDataset("dap2"))
  }
  
//  @Test 
  def write_bin_file {
    write_to_file(fof, "bin")
  }
  
  @Before
  def open_output_file {
    fos = new FileOutputStream("src/test/resources/datasets/test/ssi/b.bin")
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
  def empty_dataset = Writer(fos, "bin").write(Dataset.empty)
  
  //@Test
  def empty_function = Writer(fos, "bin").write(TestDataset.empty_function)
}