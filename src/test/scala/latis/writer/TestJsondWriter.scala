package latis.writer

import org.junit._
import Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.data.SampledData
import java.io.ByteArrayOutputStream

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
  @Test @Ignore
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
  
  @Test
  def units_metadata {
    val ds = TestDataset.canonical
    //Note: doesn't change ds model, need to test output
    //val units = ds.findVariableByName("time").get.getMetadata("units").get
    //assertEquals("milliseconds since 1970-01-01", units)
    
    //convert dataset output to string
    //TODO: utility method
    val out = new ByteArrayOutputStream()
    Writer(out, "jsond").write(ds)
    val s = out.toString().replace('\n', ' ') //new lines seem to complicate the regex
    
    val regex = """(.*units": ")([\w -]+)(".*)""".r
    val units = s match {
      case regex(a,u,b) => u
      case _ => fail
    }
    assertEquals("milliseconds since 1970-01-01", units)
    
  }
  
  @Test 
  def dataset_name {
    val ds = TestDataset.canonical
    
    val out = new ByteArrayOutputStream()
    Writer(out, "jsond").write(ds)
    val s = out.toString().replace('\n', ' ')
    
    val regex = """(^\{")([\w]+)(".*)""".r
    val dsname = s match {
      case regex(_, name, _) => name
      case _ => fail
    }
    
    assertEquals("canonical", dsname)
  }
  
  //@Test
  def test {
    val ds = getDataset("dap2")
    Writer.fromSuffix("jsond").write(ds)
  }
}