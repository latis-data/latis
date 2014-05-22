package latis.writer

import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm.Dataset
import java.io.FileOutputStream
import scala.io.Source
import latis.dm.TestNestedFunction
import latis.reader.tsml.TsmlReader
import latis.dm.Variable
import latis.metadata.Metadata
import java.io.DataOutputStream
import java.io.File

class WriterTest extends Logging {
    
  var tmpFile = java.io.File.createTempFile("LaTiS", "WriterTest")
  tmpFile.deleteOnExit
    
  val fof = TestNestedFunction.function_of_functions_with_data_in_scalars
  val tof = TestNestedFunction.tuple_of_functions
  
  val names = List("scalar", "tsi","dap2", fof, tof)

  def test_writer(ds: Dataset, suffix: String) {
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
  
  def print(name: Object, suffix: String) {
    val ds = getDataset(name)
    Writer.fromSuffix(suffix).write(ds)
  }
  
  def write_to_file(name: Object, suffix: String) {
    val ds = getDataset(name)
    val n = ds.getName
    val fos = new DataOutputStream(new FileOutputStream(new File(s"src/test/resources/datasets/data/$n/$suffix")))
    Writer(fos,suffix).write(ds)
    fos.close()
  }
  
  //@Test
  def make_test_file {
    val names = List("scalar", "tsi","dap2", fof, tof)
    val suffixes = List("asc", "bin", "csv", "das", "dds", "dods", "html", "info", "json", "jsond", "meta", "proto", "txt")
    for(name <- names)
      for(suffix <- suffixes)
        write_to_file(name, suffix)
  }
  
  //@Test
  def test {
    val w = Writer(System.out, "csv")
    logger.info("Hodwy")
    //w.write(1.0)
  }
  
}