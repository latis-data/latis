package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Tuple

/**
 * An aggregation that simply combines Datasets by reducing them to Tuples.
 * No attempt at joining is made.
 */
class CollectionAggregation extends Aggregation {
//TODO: is there a need for this now that a Dataset can't contain other Datasets?
  def aggregate(ds1: Dataset, ds2: Dataset) = {
    Dataset(Tuple(ds1.unwrap, ds2.unwrap))
  }
  
}

object CollectionAggregation {
  def apply() = new CollectionAggregation()
   
}