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
  def test_dap2 {
    test_writer(getDataset("dap2"),"asc")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"asc")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"asc")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"asc")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"asc")
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