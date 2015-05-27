package latis.ops.agg

import latis.dm.Dataset

/**
 * An aggregation that simply combines Datasets by reducing them to Tuples.
 * No attempt at joining is made.
 */
class CollectionAggregation extends Aggregation {
//TODO: is there a need for this now that a Dataset can't contain other Datasets?
  def aggregate(dataset: Dataset): Dataset = dataset
  
}

object CollectionAggregation {
  def apply() = new CollectionAggregation
}