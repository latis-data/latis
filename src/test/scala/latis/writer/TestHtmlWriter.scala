package latis.writer

import org.junit.Test

class TestHtmlWriter extends WriterTest {
  
  @Test
  def test_html {
    for(name <- names) test_writer(getDataset(name),"html")
  }
  
  //@Test
  def print_html {
    print("historical_tsi", "html")
  }
  
  //@Test 
  def write_html_file {
    for(name <- names)
    write_to_file(name, "html")
  }

}