package latis.util

import org.junit._
import Assert._
import latis.reader.DatasetAccessor
import latis.dm._
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.ops.Projection
import latis.ops.filter.FirstFilter
import latis.ops.filter.Selection

class TestCache {
  
  @Test
  def read_cache_dataset = {
    val ops = ArrayBuffer[Operation]()
    ops += Selection("name=col")
    
    val ds = DatasetAccessor.fromName("cache").getDataset(ops)
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, TupleMatch(_, Integer(n))) => assertEquals(132l, n)
      }
    }
  }
  
  @Test
  def read_cached_dataset = {
    val ds = DatasetAccessor.fromName("col").getDataset()
    //Note, we can write it twice so no TraversableOnce issue
    ds match {
      case Dataset(Function(it)) => assertEquals(3, it.length)
    }
  }
  
  @Test
  def read_cached_dataset_by_tsml_id = {
    //We otherwise assume the dataset id in the tsml matches the tsml file name.
    //Note, this also shows that we are definitely getting the dataset from the cache.
    val ds = DatasetAccessor.fromName("misnamed").getDataset()
    ds match {
      case Dataset(Function(it)) => assertEquals(3, it.length)
    }
  }
  
  @Test
  def read_cached_dataset_with_ops = {
    val ops = ArrayBuffer[Operation]()
    ops += FirstFilter()
    val ds = DatasetAccessor.fromName("col").getDataset(ops)
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_, TupleMatch(Integer(n), _, _)) => assertEquals(1, n)
      }
    }
  }
  
  @Test
  def idempotent_ops = {
    //Assert ops don't change cached data
    val ops = ArrayBuffer[Operation]()
    ops += FirstFilter()
    val ds0 = DatasetAccessor.fromName("col").getDataset(ops)
    val ds = DatasetAccessor.fromName("col").getDataset
    ds match {
      case Dataset(Function(it)) => assertEquals(3, it.length)
    }
  }
  
  @Test
  def tsml_cache = {
    //cache defined in tsml, not pre-cached
    //Entire dataset should be cached despite ops
    val ops = ArrayBuffer[Operation]()
    ops += FirstFilter()
    
    //original read/caching, ops applied after caching
    DatasetAccessor.fromName("col_cached").getDataset(ops) match {
      case Dataset(Function(it)) => assertEquals(1, it.length)
    }
    
    //read from cache, should get entire dataset
    DatasetAccessor.fromName("col_cached").getDataset() match {
      case Dataset(Function(it)) => assertEquals(3, it.length)
    }
  }
}

object TestCache {
  
  @BeforeClass
  def loadCache(): Unit = {
    DatasetAccessor.fromName("ascii_iterative").getDataset.cache
    DatasetAccessor.fromName("col").getDataset.cache
    DatasetAccessor.fromName("col_misnamed").getDataset.cache
    ()
  }
  
  @AfterClass
  def clearCache(): Unit = {
    CacheManager.clear
  }
}
