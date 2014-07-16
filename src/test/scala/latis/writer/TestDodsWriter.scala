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

class TestDodsWriter extends WriterTest{
  
  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"dods")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"dods")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"dods")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"dods")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"dods")
  }
  
  //@Test
  def print_dods {
    print(fof, "dods")
  }
  
  //@Test 
  def write_dods_file {
    write_to_file(fof, "dods")
  }
  
}
