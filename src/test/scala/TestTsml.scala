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
import latis.ops.Selection
import latis.ops.LastFilter
import latis.ops.FirstFilter
import latis.ops.LimitFilter
import latis.writer.CsvWriter

class TestTsml  {

  //@Test
  def test_variable_names {
    val tsml = Tsml("datasets/test/scalar.tsml")
    val names = tsml.getScalarNames
    assertEquals("foo", names(0))
  }
  
  //@Test
  def test_adapter_attributes {
    val xml = <dataset><adapter a="foo" b="bar"></adapter></dataset>
    val map = new DatasetMl(xml).getAdapterAttributes
    assertEquals("bar", map("b"))
  }
  
  //@Test
  def read_scalar_data {
    val reader = TsmlReader("datasets/scalar.tsml")
    //val reader = TsmlReader("datasets/test/mixed.tsml")
    val ds = reader.getDataset
    //AsciiWriter().write(ds)
    Writer("json").write(ds)
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
  
  //@Test
  def test_dataset_aggregation2 {
    //println(Tsml("datasets/test/agg.tsml#ds1"))
    //val ds = TsmlReader("datasets/test/agg.tsml#ds1").getDataset
    val ds = TsmlReader("datasets/test/agg.tsml").getDataset
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
  
  @Test
  def test_dataset {
    //val ds = TsmlReader("datasets/test/mixed.tsml").getDataset
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
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
    
    Writer("csv").write(ds)
  }
  
  //@Test
  def test_dbstats {
    val ds = TsmlReader("/home/lindholm/git/dbstats/src/main/resources/datasets/kepler_usage.tsml").getDataset
    AsciiWriter().write(ds)
  }
}