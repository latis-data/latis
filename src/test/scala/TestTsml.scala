import latis.dm._
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import latis.reader.tsml._

class TestTsml  {

  @Test
  def test_variable_names {
    val tsml = Tsml("datasets/test/scalar.tsml")
    val names = tsml.getVariableNames
    assertEquals("foo", names(0))
  }
  
  @Test
  def test_adapter_attributes {
    val xml = <dataset><adapter a="foo" b="bar"></adapter></dataset>
    val map = new DatasetMl(xml).getAdapterAttributes
    assertEquals("bar", map("b"))
  }
  
  @Test
  def read_scalar_data {
    val reader = TsmlReader("datasets/test/scalar.tsml")
    val ds = reader.getDataset
    println(ds)
    
    AsciiWriter().write(ds)
    //new JsonWriter(System.out).write(ds)
  }
  
  @Test
  def read_scalar_data_tuple_range {
    val ds = TsmlReader("datasets/test/scalar3.tsml").getDataset //+ 1
    //println(ds)
    
    Writer("hc").write(ds)
    //new JsonWriter(System.out).write(ds)
  }
  
  //@Test
  def read_jdbc_data {
    val ds = TsmlReader("datasets/test/db.tsml").getDataset /// 1000000.
    
    AsciiWriter().write(ds)
  }
}