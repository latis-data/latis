package latis.writer

import java.io.FileOutputStream
import org.junit._
import Assert._
import scala.io.Source
import latis.reader.tsml.TsmlReader
import java.io.DataOutputStream
import java.io.File
import latis.dm._
import latis.metadata.Metadata

class TestDapWriters extends WriterTest{
  
  @Test
  def test_dap {
    for(name <- names) {
      test_writer(getDataset(name),"das")
      test_writer(getDataset(name),"dds")
      test_writer(getDataset(name),"dods")
    }
  }
  
  //@Test
  def print_dap {
    print(fof, "dds")
  }
  
  //@Test 
  def write_dap_file {
    write_to_file(fof, "dds")
  }
  
}
