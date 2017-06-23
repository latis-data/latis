package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Tuple

/**
 * Assumes aggregates have the same type and domain sets
 * don't overlap and are ordered.
 */
class TileAggregation extends Aggregation { 
  
  /*
   * TODO: enforce that all datasets have the same set of vars
   * auto intersect? keep only common vars?
   * or union, fill with NaN?
   * 
   * make sure domain units are the same
   * convert to units of the first
   */
  
  def aggregate(ds1: Dataset, ds2: Dataset): Dataset = {
    val (f1, f2, it1, it2) = (ds1, ds2) match {
      case(Dataset(f1 @ Function(it1)), Dataset(f2 @ Function(it2))) => (f1, f2, it1, it2)
      case _ => throw new UnsupportedOperationException("TileAggregation expects a Function in each of the Datasets it aggregates.")
    }
    val iterator = it1 ++ it2
    
    //assume same type for each Function
    val domain = f1.getDomain
    val range = f2.getRange
    val f = Function(domain, range, iterator)
    
    //TODO: munge metadata
    
    Dataset(f)
  }
  
}

object TileAggregation {
  def apply(): TileAggregation = new TileAggregation()
  def apply(ds1: Dataset, ds2: Dataset): Dataset = new TileAggregation()(ds1, ds2)
}