package latis.writer

import org.junit._
import latis.reader.tsml.TsmlReader
import latis.ops.filter.Selection

class TestInfoWriter extends WriterTest{
  
  @Test
  def test_info {
    for(name <- names) test_writer(getDataset(name),"info")
  }
  
  //@Test
  def print_info {
    print(fof, "info")
  }
  
  //@Test 
  def write_info_file {
    for(name <- names)
    write_to_file(name, "info")
  }

}