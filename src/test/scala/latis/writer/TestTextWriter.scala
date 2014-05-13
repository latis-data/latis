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

class TestTextWriter extends WriterTest{

  @Test
  def test_txt {
    for(name <- names) test_writer(getDataset(name),"txt")
  }
  
  //@Test
  def print_txt {
    print(fof, "txt")
  }
  
  //@Test 
  def write_txt_file {
    for(name <- names)
    write_to_file(name, "txt")
  }
  
}