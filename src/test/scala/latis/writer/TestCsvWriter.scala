package latis.writer

import org.junit._
import Assert._
import latis.dm._

class TestCsvWriter extends WriterTest{
  
  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"csv")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"csv")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"csv")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"csv")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"csv")
  }
  
  //@Test
  def print_csv {
    print(fof, "csv")
  }
  
  //@Test 
  def write_csv_file {
    write_to_file(fof, "csv")
  }
  
}