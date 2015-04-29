package latis.writer

import org.junit._

class TestHtmlWriter extends WriterTest {
  
  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"html")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"html")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"html")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"html")
  }
  
  //TODO: tuple of functions: should we make a plot for each, breaks now because it looks for top level function
  @Test @Ignore
  def test_tof {
    test_writer(getDataset(tof),"html")
  }
  
  //@Test
  def print_html {
    print("historical_tsi", "html")
  }
  
  //@Test 
  def write_html_file {
    write_to_file(fof, "html")
  }

}