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
    //Add 2 datasets to the cache (both same size) and check the size in the 
    // "cache" dataset
    val ds1 = DatasetAccessor.fromName("ascii_iterative").getDataset
    val ds2 = DatasetAccessor.fromName("col").getDataset
    CacheManager.cacheDataset(ds1)
    CacheManager.cacheDataset(ds2)
    val ds3 = DatasetAccessor.fromName("cache").getDataset
    CacheManager.clear //clean up for other tests
    
    ds3 match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_,Tuple(vars)) => vars(1) match {case Integer(n) => assertEquals(132l, n)}
      }
    }
  }  
  
  @Test
  def read_cache_dataset_with_ops = {
    //Add 2 datasets to the cache (both same size) and check the size in the 
    // "cache" dataset
    val ds1 = DatasetAccessor.fromName("ascii_iterative").getDataset match {
      case ds @ Dataset(v) => Dataset(v, ds.getMetadata + ("creation_time" -> System.currentTimeMillis.toString))
    }
    val ds2 = DatasetAccessor.fromName("col").getDataset match {
      case ds @ Dataset(v) => Dataset(v, ds.getMetadata + ("creation_time" -> System.currentTimeMillis.toString))
    }
    
    CacheManager.cacheDataset(ds1)
    CacheManager.cacheDataset(ds2)
    
    val ops = ArrayBuffer[Operation]()
    ops += Selection("name=col")
    //ops += Projection("name")
    //ops += FirstFilter()
    
    val ds = DatasetAccessor.fromName("cache").getDataset(ops)
    CacheManager.clear //clean up for other tests
    
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(_,Tuple(vars)) => vars(1) match {case Integer(l) => assertEquals(132, l)}
      }
    }
  }
}