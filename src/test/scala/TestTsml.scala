import latis.dm._
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import latis.reader.tsml._
import latis.reader.tsml.ml._
import latis.ops.Projection
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.ops.filter._
import latis.writer.CsvWriter
import scala.io.Source

import org.junit._
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import latis.dm._
import latis.writer.Writer

class TestTsml  {

  //@Test
//  def test_variable_names {
//    val tsml = Tsml("datasets/test/scalar.tsml")
//    val names = tsml.getScalarNames
//    assertEquals("a", names(1))
//  }
  
  //@Test
  def test_adapter_attributes {
    val xml = <dataset><adapter a="foo" b="bar"></adapter></dataset>
    val map = new DatasetMl(xml).getAdapterAttributes
    assertEquals("bar", map("b"))
  }
  
  //@Test
  def read_scalar_data_tuple_range {
    val ds = TsmlReader("datasets/test/scalar3.tsml").getDataset //+ 1
    //println(ds)
    //AsciiWriter().write(ds)
    val w = new MetadataWriter
    val s = w.varToString(ds)
    println(s)
  }
  
  //@Test
  def read_jdbc_data {
    val ds = TsmlReader("datasets/test/db.tsml").getDataset /// 1000000.
    
    AsciiWriter().write(ds)
  }
  
  //@Test
  def test_dataset_aggregation {
    val ds1 = TsmlReader("datasets/test/scalar.tsml").getDataset
    val ds2 = TsmlReader("datasets/test/scalar3.tsml").getDataset
    
    val ds = Dataset(ds1, ds2)
    AsciiWriter().write(ds)
  }
  
 // @Test
  def test_dataset_aggregation2 {
    //TODO: broken
    //println(Tsml("datasets/test/agg.tsml#ds1"))
    //val ds = TsmlReader("datasets/test/agg/agg.tsml#ds1").getDataset
    val ds = TsmlReader("datasets/test/agg/agg.tsml").getDataset
    AsciiWriter().write(ds)
  }
  
  //@Test
  def test_log_file {
    val ds = TsmlReader("datasets/test/log.tsml").getDataset
    AsciiWriter().write(ds)
  }
  
  //@Test
  def test_columnar_file {
    val ds = TsmlReader("datasets/test/col.tsml").getDataset
    AsciiWriter().write(ds)
  }
  
  //@Test
  def test_file_list {
    val ds = TsmlReader("datasets/test/files.tsml").getDataset
    AsciiWriter().write(ds)
  }
  
  //@Test
  def test_nested_file_list {
    val ds = TsmlReader("datasets/test/dirs.tsml").getDataset
    AsciiWriter().write(ds)
  }
  
//  @Test
//  def test_mms_file_list {
//    val ops = ArrayBuffer[Operation]()
//    ops += Selection("data_level=ql")
//    ops += LimitFilter(3)
//    //ops += Projection("time,data_rate_mode,file")
//    //ops += new FirstFilter
//    val ds = TsmlReader("/home/lindholm/git/latis-mms/src/main/webapp/datasets/science_files.tsml").getDataset(ops)
//    AsciiWriter().write(ds)
//  }
  
  //@Test
  def test_dataset {
    //val ds = TsmlReader("datasets/test/mixed.tsml").getDataset
    val ds = TsmlReader("datasets/tsi.tsml").getDataset
    AsciiWriter().write(ds)
    //Writer("json").write(ds)
  }
  
  //@Test
  def test_ldap {
    //TODO: index name "unknown"
    //TODO: use "sequence"
    val ops = ArrayBuffer[Operation]()
    ops += Selection("memberOf=~.*maven-sdc-web-users.*")
    ops += Projection("uid,cn,mail")
    val ds = TsmlReader("datasets/test/ldap_users.tsml").getDataset(ops)
    
    Writer.fromSuffix("csv").write(ds)
  }
  
  //@Test
  def test {
    val ops = List(Operation("last"))
    val ds = TsmlReader("/home/lindholm/git/lisird3/src/test/resources/datasets/test/sorce_tim_tsi_6h_v12.tsml").getDataset(ops)
    Writer.fromSuffix("asc").write(ds)//2452696.0
  }
}
