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
    
//  @Test
//  def test_dap2 {
//    test_writer(getDataset("dap2"),"bin")
//  }
//  @Test
//  def test_fof {
//    test_writer(getDataset(fof),"bin")
//  }
//  @Test
//  def test_scalar {
//    test_writer(getDataset("scalar"),"bin")
//  }
//  @Test
//  def test_tsi {
//    test_writer(getDataset("tsi"),"bin")
//  }
//  @Test
//  def test_tof {
//    test_writer(getDataset(tof),"bin")
//  }
  
  //@Test
  def print_bytes {
    val w = new BinaryWriter
    val ds = getDataset(fof)
    for(v <- ds.getVariables) {
      for(b <- w.varToBytes(v)) println(b.toInt.toHexString.replace("ffffff", ""))
    }
    AsciiWriter.write(getDataset("dap2"))
  }
  
//  @Test 
  def write_bin_file {
    write_to_file(fof, "bin")
  }
  
  @Before
  def open_output_file {
    fos = new FileOutputStream("src/test/resources/datasets/test/ssi/a.bin")
  }
  
  @After
  def close_output_file {
    fos.close
  }
  
  def a = Dataset(Binary(ByteBuffer.allocate(8*9).putLong(0).putLong(0).putLong(0).putLong(1).putLong(1).putLong(1).putLong(2).putLong(2).putLong(2)))
  def b = Dataset(Binary(ByteBuffer.allocate(8*9).putLong(1).putLong(2).putLong(3).putLong(1).putLong(2).putLong(3).putLong(1).putLong(2).putLong(3)))
  def c = Dataset(Binary(ByteBuffer.allocate(8*9).putLong(10).putLong(11).putLong(12).putLong(13).putLong(14).putLong(15).putLong(16).putLong(17).putLong(18)))
  
//  @Test
  def write {
    BinaryWriter(fos).write(a)
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