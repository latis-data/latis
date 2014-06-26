package latis.writer

import org.junit.Test

class TestProtoWriter extends WriterTest {
  
  @Test
  def test_proto {
    for(name <- names) test_writer(getDataset(name),"proto")
  }
  
  //@Test
  def print_proto {
    print("dap2", "proto")
  }
  
  //@Test 
  def write_proto_file {
    for(name <- names)
    write_to_file(name, "proto")
  }

}