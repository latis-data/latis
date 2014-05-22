package latis.writer

import org.junit.Test

class TestProtobufWriter extends WriterTest {
  
  //@Test
  def test_proto {
    for(name <- names) test_writer(getDataset(name),"proto")
  }
  
  //@Test
  def print_proto {
    print(fof, "proto")
  }
  
  //@Test 
  def write_proto_file {
    for(name <- names)
    write_to_file(name, "proto")
  }

}