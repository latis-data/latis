package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.dm.TestDataset
import latis.ops.filter.FirstFilter

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
}