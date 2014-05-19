package latis.writer

import org.junit._
import Assert._
import latis.dm._

class TestJsonWriters extends WriterTest {

  @Test
  def test_json {
    for(name <- names) test_writer(getDataset(name),"json")
  }
  
  @Test
  def test_jsond {
    for(name <- names) test_writer(getDataset(name),"jsond")
  }
  
  //@Test
  def print_json {
    print(fof, "json")
  }
  
  //@Test 
  def write_json_file {
    //for(name <- names)
    write_to_file(fof, "jsond")
  }
  
  def real = Writer.fromSuffix("json").write(TestDataset.real)
  def integer = Writer.fromSuffix("json").write(TestDataset.integer)
  def text = Writer.fromSuffix("json").write(TestDataset.text)
  
  //@Test
  def testj = Writer.fromSuffix("json").write(TestDataset.index_function)
  
  //@Test 
  def empty_dataset = Writer.fromSuffix("json").write(TestDataset.empty)
  
  //@Test
  def empty_function = Writer.fromSuffix("json").write(TestDataset.empty_function)
}