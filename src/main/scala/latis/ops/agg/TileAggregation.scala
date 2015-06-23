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
  
  def aggregate(ds1: Dataset, ds2: Dataset) = {
    val functions = List(ds1, ds2).flatMap(_.unwrap.findFunction)
    val iterator = functions.foldLeft(Iterator[Sample]())(_ ++ _.iterator)
    
    //assume same type for each Function
    val domain = functions.head.getDomain
    val range = functions.head.getRange
    val f = Function(domain, range, iterator)
    
    //TODO: munge metadata
    
    Dataset(f)
  }
  
}

object TileAggregation {
  def apply() = new TileAggregation()
}