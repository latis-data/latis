package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test


class TestCatalogReader {
  
  @Test
  def flat {
    val ds = FlatCatalogReader().getDataset
    val data = ds.toStringMap
    assertEquals(data("name")(0), "agg/agg")
    assertEquals(data("accessURL")(0), "agg/agg")
  }
  
  @Test
  def nested {
    val ds = CatalogReader().getDataset
    val data = ds.toStringMap
    assertEquals(data("name")(0), "agg")
    assertEquals(data("accessURL")(0), "agg")   
    assert(!data("name").contains("ssi"))
  }
  
}