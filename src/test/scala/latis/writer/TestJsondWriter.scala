package latis.writer

import org.junit._
import Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.data.SampledData

class TestJsondWriter extends WriterTest {

  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"jsond")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"jsond")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"jsond")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"jsond")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"jsond")
  }
  
  //@Test
  def print_json {
    print(fof, "json")
  }
  
  //@Test 
  def write_json_file {
    //for(name <- names)
    write_to_file(fof, "json")
  }
}