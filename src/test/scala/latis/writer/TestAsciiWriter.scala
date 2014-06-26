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

class TestAsciiWriter extends WriterTest{
  
  @Test
  def test_ascii {
    for(name <- names) test_writer(getDataset(name),"asc")
  }
  
  //@Test
  def print_asc {
    print(fof, "asc")
  }
  
  //@Test 
  def write_asc_file {
    write_to_file(fof, "asc")
  }
  
}