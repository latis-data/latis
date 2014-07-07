package latis.writer

import org.junit.Test

class TestProtoWriter extends WriterTest {
  
  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"proto")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"proto")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"proto")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"proto")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"proto")
  }
  
  //@Test
  def print_proto {
    print("dap2", "proto")
  }
  
  //@Test 
  def write_proto_file {
    write_to_file(fof, "proto")
  }

}