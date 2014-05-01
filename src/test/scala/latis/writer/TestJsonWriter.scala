package latis.writer

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.dm.TestDataset
import java.io.FileOutputStream
import latis.reader.tsml.TsmlReader
import scala.io.Source
import java.io.DataOutputStream
import java.io.File
import latis.dm._

class TestJsonWriter {
  
  var tmpFile = java.io.File.createTempFile("json", "test")
  tmpFile.deleteOnExit
  
  val TsmlNames = List("scalar", "tsi","dap2")
 
  val fof = Dataset(TestNestedFunction.function_of_functions,Metadata("function_of_functions"))
  val tof = Dataset(TestNestedFunction.tuple_of_functions,Metadata("tuple_of_functions"))
  val datasets = List(fof, tof)
    
  @Test
  def test_tsmls {
    for(name <- TsmlNames) {
    	test(getTsmlDataset(name),"json")
    }
  }
  
  @Test
  def test_others {
    for(ds <- datasets){
      test(ds,"json")
    }
  }
  
  def test(ds: Dataset, suffix: String) {
    val fos = new FileOutputStream(tmpFile)
    val name = ds.getName
    Writer(fos,suffix).write(ds)
    fos.close()
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/$name/$suffix").getLines
    while(t.hasNext) assertEquals(t.next, s.next)
    assert(s.isEmpty)
  }
  
  def getTsmlDataset(name: String): Dataset = TsmlReader(s"datasets/test/$name.tsml").getDataset
  
  
  //@Test
  def print_json {
    val reader = TsmlReader("datasets/test/scalar.tsml")
    //val ds = reader.getDataset()
    val ds = tof
    Writer.fromSuffix("json").write(ds)
  }
  
  //@Test 
  def write_to_file {
    val fos = new DataOutputStream(new FileOutputStream(new File("src/test/resources/datasets/data/tuple_of_functions/json")))
    //val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val ds = tof
    Writer(fos,"json").write(ds)
    fos.close()
  }
  
  def real = Writer.fromSuffix("json").write(TestDataset.real)
  def integer = Writer.fromSuffix("json").write(TestDataset.integer)
  def text = Writer.fromSuffix("json").write(TestDataset.text)
  
  //@Test
  def test = Writer.fromSuffix("json").write(TestDataset.index_function)
  
  //@Test 
  def empty_dataset = Writer.fromSuffix("json").write(TestDataset.empty)
  
  //@Test
  def empty_function = Writer.fromSuffix("json").write(TestDataset.empty_function)
}