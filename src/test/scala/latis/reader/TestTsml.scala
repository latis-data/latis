package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.DatasetMl

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
    assert(ds.toSeq(0).getMetadata("alias").isDefined)
    assert(!ds.toSeq(1).getMetadata("alias").isDefined)
//    Writer.fromSuffix("meta").write(ds)
  }
}