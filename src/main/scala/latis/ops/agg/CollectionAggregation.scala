package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Tuple

/**
 * An aggregation that simply combines Datasets by reducing them to Tuples.
 * No attempt at joining is made.
 */
class CollectionAggregation extends Aggregation {
//TODO: is there a need for this now that a Dataset can't contain other Datasets?
  def aggregate(ds1: Dataset, ds2: Dataset) = (ds1, ds2) match {
    case (Dataset(v), Dataset(u)) => Dataset(Tuple(v, u))
    // If one or both datasets are empty, should we return a totally empty one?
    // Or should we return an "incomplete" Dataset with one of the tuples being
    // a null, following the convention of unwrap/Dataset.empty?
    case (_, _) => Dataset.empty
  }
  
}

object CollectionAggregation {
  def apply() = new CollectionAggregation()
  def apply(ds1: Dataset, ds2: Dataset) = new CollectionAggregation()(ds1, ds2)
}
