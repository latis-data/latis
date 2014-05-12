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

class TestAsciiWriter {
  
  var tmpFile = java.io.File.createTempFile("asc", "test")
  tmpFile.deleteOnExit
  
  val fof = TestNestedFunction.function_of_functions
  val tof = TestNestedFunction.tuple_of_functions
  
  val names = List("scalar", "tsi","dap2", fof, tof)
  
  @Test
  def test_sets {
    for(name <- names) test_asc(getDataset(name),"asc")
  }
  
  def test_asc(ds: Dataset, suffix: String) {
    val fos = new FileOutputStream(tmpFile)
    val name = ds.getName
    Writer(fos,suffix).write(ds)
    fos.close()
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/$name/$suffix").getLines
    while(t.hasNext) assertEquals(t.next, s.next)
    assert(s.isEmpty)
  }
  
  def getDataset(name: Object): Dataset = name match {
    case s: String => TsmlReader(s"datasets/test/$s.tsml").getDataset
    case v: Variable => Dataset(v,Metadata(v.getName))
  }
  
  
  //@Test
  def print_asc {
    val reader = TsmlReader("datasets/test/dap2.tsml")
    //val ds = reader.getDataset()
    val ds = getDataset(tof)
    Writer.fromSuffix("asc").write(ds)
  }
  
  //@Test 
  def write_to_file {
    val fos = new DataOutputStream(new FileOutputStream(new File("src/test/resources/datasets/data/function_of_functions/asc")))
    //val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val ds = getDataset(fof)
    Writer(fos,"asc").write(ds)
    fos.close()
  }
}