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

class TestMetadataWriter extends WriterTest{
  
  @Test
  def test_meta {
    for(name <- names) test_writer(getDataset(name),"meta")
  }
  
  //@Test
  def print_meta {
    print("scalar", "meta")
  }
  
  //@Test 
  def write_meta_file {
    for(name <- names)
    write_to_file(name, "meta")
  }
  
}