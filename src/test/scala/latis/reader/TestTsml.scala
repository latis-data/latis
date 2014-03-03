package latis.reader

import latis.reader.tsml.ml._
import org.junit._
import Assert._

class TestTsml  {

  @Test
  def test_variable_names {
    val tsml = Tsml("datasets/test/col.tsml")
    val names = tsml.getScalarNames
    assertEquals("myTime", names.head)
  }
  
  @Test
  def test_adapter_attributes {
    val xml = <dataset><adapter a="foo" b="bar"></adapter></dataset>
    val map = new DatasetMl(xml).getAdapterAttributes
    assertEquals("bar", map("b"))
  }
}