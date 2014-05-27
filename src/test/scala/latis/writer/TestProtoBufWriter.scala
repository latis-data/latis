package latis.writer

import org.junit.Test
import latis.dm._
import java.nio.ByteBuffer

class TestProtoBufWriter extends WriterTest{

  //@Test
  def test_protod {
    for(name <- names) test_writer(getDataset(name),"protod")
  }
  
  //@Test
  def print_protod {
    print("dap2", "protod")
  }
  
  //@Test 
  def write_protod_file {
    for(name <- names)
    write_to_file(name, "protod")
  }  
  
  //@Test
  def print_bytes {
    val w = new ProtoBufWriter
    val ds = Dataset(Tuple(Integer(150), Text("testing")))//getDataset(fof)
    for(v <- ds.getVariables) {
      val a = w.varToBytes(v)
      for(b <- a) println(b.toInt.toHexString.replaceAll("ffffff",""))
    }
  }

}