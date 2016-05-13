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
import latis.reader.DatasetAccessor

class TestTextWriter extends WriterTest{

  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"txt")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"txt")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"txt")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"txt")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"txt")
  }
  
  //@Test
  def print_txt {
    print(fof, "txt")
  }
  
  //@Test 
  def write_txt_file {
    write_to_file(fof, "txt")
  }
  
  @Test
  def write_with_precision = {
    val ds = DatasetAccessor.fromName("ascii_precision").getDataset
    //latis.writer.TextWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, s:Scalar) => {
          println(s.getMetadata("precision"))
          assertEquals("0.12", latis.writer.TextWriter.makeScalar(s))
        }
      }
    }
  }
}
