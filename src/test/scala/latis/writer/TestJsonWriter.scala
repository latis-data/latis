package latis.writer

import org.junit._
import Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.data.SampledData

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
  
  //@Test
  def missing_value {
    val domain = Real(Metadata(Map("name" -> "domain")))
    val range = Real(Metadata(Map("name" -> "range", "missing_value" -> "0")))
    val data = SampledData.fromValues(List(0,1,2,3), List(1,2,0,4))
    val ds = Dataset(Function(domain, range, data = data))
    
    //Writer.fromSuffix("csv").write(ds)
    Writer.fromSuffix("jsond").write(ds)
  }
}