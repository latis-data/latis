package latis.writer

import org.junit.Test
import latis.dm._
import java.nio.ByteBuffer

class TestProtoBufWriter extends WriterTest{

  @Test
  def test_protod {
    val names = List("tsi")
    for(name <- names) test_writer(getDataset("tsi"),"protod")
  }
  
  //@Test
  def print_protod {
    print("dap2", "protod")
  }
  
  //@Test 
  def write_protod_file {
    //for(name <- names)
    write_to_file("tsi", "protod")
  }  
  
  //@Test
  def print_bytes {
    val w = new ProtoBufWriter
    val ds = getDataset("scalar")
    for(v <- ds.getVariables) {
      for(b <- w.varToBytes(v)) println(b.toInt.toHexString.replaceAll("ffffff",""))
    }
  }

}