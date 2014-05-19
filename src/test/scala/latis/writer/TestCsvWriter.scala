package latis.writer

import org.junit._
import Assert._
import latis.dm._

class TestCsvWriter extends WriterTest{
  
  @Test
  def test_csv {
    for(name <- names) test_writer(getDataset(name),"csv")
  }
  
  //@Test
  def print_csv {
    print(fof, "csv")
  }
  
  //@Test 
  def write_csv_file {
    for(name <- names)
    write_to_file(name, "csv")
  }
  
}