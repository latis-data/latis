package latis.writer

import org.junit.Test
import latis.dm._
import java.nio.ByteBuffer

class TestProtoBufWriter extends WriterTest{

  @Test
  def test_dap {
    test_writer(getDataset("dap2"),"protod")
  }
  
  @Test
  def test_fof {
    test_writer(getDataset(fof),"protod")
  }
  
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"protod")
  }
  
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"protod")
  }
  
  @Test
  def test_tof {
    test_writer(getDataset(tof),"protod")
  }
  
  //@Test
  def print_protod {
    print("dap2", "protod")
  }
  
  //@Test 
  def write_protod_file {
    //for(name <- names)
    write_to_file(fof, "protod")
  }  
  
  //@Test
  def print_bytes {
    val w = new ProtoBufWriter
    val ds = getDataset(fof)
    val v = ds.unwrap
    for(b <- w.varToBytes(v)) println(b.toInt.toHexString.replaceAll("ffffff",""))
    
    AsciiWriter.write(getDataset(fof))
  }

}