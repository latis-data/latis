package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test


class TestCatalogReader {
  
  @Test
  def flat {
    val ds = FlatCatalogReader().getDataset
    val data = ds.toStringMap
    //assertEquals("agg/agg", data("name")(0))
    //assertEquals("agg/agg", data("accessURL")(0))
    assertEquals("agg", data("name")(0))
    assertEquals("agg", data("accessURL")(0))
  }
  
  @Test
  def nested {
    val ds = CatalogReader().getDataset()
    val data = ds.toStringMap
    assertEquals("agg", data("name")(0))
    assertEquals("agg", data("accessURL")(0))   
    assert(!data("name").contains("ssi"))
  }
  
}