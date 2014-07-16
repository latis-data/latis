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

class TestDasWriter extends WriterTest{
  
  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"das")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"das")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"das")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"das")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"das")
  }
  
  //@Test
  def print_das {
    print(fof, "das")
  }
  
  //@Test 
  def write_das_file {
    write_to_file(fof, "das")
  }
  
}
