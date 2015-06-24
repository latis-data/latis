package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.ops.agg.CollectionAggregation
import latis.reader.tsml.ml.Tsml

/**
 * Delegate to the CollectionAggregation Operation to combine all the 
 * datasets as Tuples with their own namespace with no attempt to join them.
 */
class CollectionAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {

  def aggregate(left: Dataset, right: Dataset): Dataset = CollectionAggregation(left, right)
  
}