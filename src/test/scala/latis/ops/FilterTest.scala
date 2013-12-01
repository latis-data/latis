package latis.ops

import latis.dm._
import latis.writer.AsciiWriter
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import scala.collection._
import scala.collection.mutable.ArrayBuffer

class FilterTest {

  val dataset = Dataset(Function.fromValues(List(1.1,2.2,3.3), List(4.4,5.5,6.6)))
  
  //@Test
  def test_last_filter {
    val ds = LastFilter(dataset)
    AsciiWriter.write(ds)
  }
  
  //@Test
  def test_first_filter {
    val ds = FirstFilter(dataset)
    AsciiWriter.write(ds)
  }
  
  @Test
  def test_last_from_iterative_ascii_file {
    val ds1 = TsmlReader("datasets/test/scalar.tsml").getDataset
    val ds2 = LastFilter(ds1)
    AsciiWriter.write(ds2)
  }
  
  //@Test
  def test_last_from_database {
    val ops = ArrayBuffer[Operation]()
    ops += Selection("orbit_number=983")
    val ds1 = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    //val dataset = TsmlReader("datasets/test/scalar.tsml").getDataset
    val ds2 = LastFilter(ds1)
    AsciiWriter.write(ds2)
  }

}