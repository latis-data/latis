package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample

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
  
  def aggregate(dataset: Dataset): Dataset = {
    //assume one-dimension (e.g. time), for now
    //assume each dataset contains only one top level variable, the Function
    
    val datasets = dataset.getVariables
    val functions = datasets.map(_.asInstanceOf[Dataset].getVariables.head.asInstanceOf[Function])
    val iterator = functions.foldLeft(Iterator[Sample]())(_ ++ _.iterator)
    
    //assume same type for each Function
    val domain = functions.head.getDomain
    val range = functions.head.getRange
    val f = Function(domain, range, iterator)
    
    //TODO: munge metadata
    
    Dataset(f, dataset.metadata)
  }
  
}

object TileAggregation {
  def apply() = new TileAggregation
}