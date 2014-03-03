package latis.ops

import latis.dm._
import latis.data.IterableData
import latis.data.Data
import latis.dm.Sample
import latis.util.PeekIterator

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
    
    val ds = Dataset(f, dataset.metadata)
    ds
    /*
     * TODO: 2014-03-03
     * this is being generated for the origDataset
     * the makeDataset pass will make a new Dataset but it misses that this function already has an iterator
     * in TsmlAdapter.makeFunction
     * 
     * 
     * are there other implications of the two pass construction of Datasets for aggregations?
     * makes sense to do some aggregation logic in the 1st pass since we need to present a single dataset
     * but we shouldn't add/apply operations until the 2nd pass?
     * should 1st pass just give us the collection?
     * 
     */
  }
  
}

object TileAggregation {
  def apply() = new TileAggregation
}