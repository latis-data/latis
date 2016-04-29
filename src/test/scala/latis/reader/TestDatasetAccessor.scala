package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.dm.TestDataset
import latis.ops.filter.FirstFilter
import latis.dm.Dataset
import latis.dm.Real
import latis.metadata.Metadata

class TestDatasetAccessor {
  
  @Test
  def dataset_wrapper_without_ops = {
    val reader = DatasetAccessor(TestDataset.canonical)
    val ds = reader.getDataset()
    assertEquals(3, ds.getLength)
  }
  
  @Test
  def dataset_wrapper_with_ops = {
    val reader = DatasetAccessor(TestDataset.canonical)
    val ds = reader.getDataset(List(FirstFilter()))
    assertEquals(1, ds.getLength)
  }
  
  @Test
  def dataset_from_cache = {
    Dataset(Real(3.14), Metadata("pi")).cache
    DatasetAccessor.fromName("pi").getDataset() match {
      case Dataset(Real(d)) => assertEquals(3.14, d, 0.0)
    }
  }
  
}