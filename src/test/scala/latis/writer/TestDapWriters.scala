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

class TestDapWriters {

  var tmpFile = java.io.File.createTempFile("writer", "test")
  tmpFile.deleteOnExit
    
  val TsmlNames = List("scalar", "tsi","dap2")
 
  val fof = Dataset(TestNestedFunction.function_of_functions,Metadata("function_of_functions"))
  val tof = Dataset(TestNestedFunction.tuple_of_functions,Metadata("tuple_of_functions"))
  val datasets = List(fof, tof)
    
  @Test
  def test_tsmls {
    for(name <- TsmlNames) {
      test(getTsmlDataset(name),"dds")
      test(getTsmlDataset(name),"das")
      test(getTsmlDataset(name),"dods")
      test(getTsmlDataset(name),"asc")
      test(getTsmlDataset(name),"bin")
    }
  }
  
  @Test
  def test_others {
    for(ds <- datasets){
      test(ds,"dds")
      test(ds,"das")
      test(ds,"dods")
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
  def write_dds {
    val reader = TsmlReader("datasets/test/historical_tsi.tsml")
    val ds = reader.getDataset()
    //val ds = Dataset(TestNestedFunction.tuple_of_functions) 
    Writer.fromSuffix("dds").write(ds)
  }
    
  //@Test
  def write_das {
    val reader = TsmlReader("datasets/test/dap2.tsml")
    val ds = reader.getDataset
    Writer.fromSuffix("das").write(ds)
  }
  
  //@Test 
  def write_dods {
    val fos = new DataOutputStream(new FileOutputStream(new File("src/test/resources/datasets/data/tuple_of_functions/asc")))
    //val ds = TsmlReader("datasets/test/tsi.tsml").getDataset
    val ds = Dataset(TestNestedFunction.tuple_of_functions,Metadata("tuple_of_functions")) 
    Writer(fos,"asc").write(ds)
    fos.close()
  }
}
