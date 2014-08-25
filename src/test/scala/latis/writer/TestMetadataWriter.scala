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
  def test_dap2 {
    test_writer(getDataset("dap2"),"meta")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"meta")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"meta")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"meta")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"meta")
  }
  
  //@Test
  def print_meta {
    print("scalar", "meta")
  }
  
  //@Test 
  def write_meta_file {
    write_to_file(fof, "meta")
  }
  
  //@Test
  def test {
    val ds = getDataset(tof) //TestDataset.canonical
    Writer.fromSuffix("meta").write(ds)
  }
}