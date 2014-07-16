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

class TestDdsWriter extends WriterTest{
  
  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"dds")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"dds")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"dds")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"dds")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"dds")
  }
  
  //@Test
  def print_dds {
    print(fof, "dds")
  }
  
  //@Test 
  def write_dds_file {
    write_to_file(fof, "dds")
  }
  
}
