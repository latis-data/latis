package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import latis.dm.Dataset
import latis.metadata.Catalog
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.DatasetMl
import latis.reader.tsml.ml.TsmlResolver
import scala.xml.UnprefixedAttribute
import scala.xml.Null
import latis.reader.tsml.ml.Tsml

class TestTsml  {

//  @Test
//  def test_variable_names {
//    val tsml = Tsml("datasets/test/col.tsml")
//    val names = tsml.getScalarNames
//    assertEquals("myTime", names.head)
//  }
  
  @Test
  def test_adapter_attributes {
    val xml = <dataset><adapter a="foo" b="bar"></adapter></dataset>
    val map = new DatasetMl(xml).getAdapterAttributes
    assertEquals("bar", map("b"))
  }
  
  @Test
  def two_time {
    val ds = TsmlReader("datasets/test/two_time.tsml").getDataset
    ds match {
      case Dataset(v) => {
        assert(v.toSeq(0).getMetadata("alias").isDefined)
        assert(!v.toSeq(1).getMetadata("alias").isDefined)
      }
      case _ => fail()
    }
//    Writer.fromSuffix("meta").write(ds)
  }
  
  @Test
  def resolve_url {
    val url = Catalog.getTsmlUrl("properties")    
    val tsml = TsmlResolver.fromUrl(url)
    assert(tsml.toString.contains("""id="properties""""))
  }
  
  @Test
  def resolve_path {
    val path = "datasets/test/properties.tsml"
    val tsml = TsmlResolver.fromPath(path)
    assert(tsml.toString.contains("""id="properties""""))
  }
  
  @Test
  def resolve_name {
    val name = "properties"
    val tsml = TsmlResolver.fromName(name)
    assert(tsml.toString.contains("""id="properties""""))
  }
  
  @Test
  def set_location {
    val tsml = Tsml(<dataset><adapter location="foo"/></dataset>)
    val t2 = tsml.dataset.setLocation("bar")
    assert(t2.toString.contains("""location="bar""""))
  }
  
}
