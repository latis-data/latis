package latis.ops

import latis.dm._
import latis.data.IterableData
import latis.data.Data
import latis.dm.Sample
import latis.util.NextIterator

/**
 * Assumes aggregates have the same type and domain sets
 * don't overlap and are ordered.
 */
class TileAggregation extends Aggregation {
  
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
    
    Dataset(List(f), dataset.metadata)
  }
  
}

object TileAggregation {
  def apply() = new TileAggregation
}