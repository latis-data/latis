package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Tuple

/**
 * An aggregation that simply combines Datasets by reducing them to Tuples.
 * No attempt at joining is made.
 */
class CollectionAggregation extends Aggregation {

  def aggregate(ds1: Dataset, ds2: Dataset): Dataset = (ds1, ds2) match {
    //TODO: we should the variable of either or both. 
    //  It seems that we do this somewhere. 
    //  Let's punt since this will be deprecated in favor of Join.
    case (Dataset(v), Dataset(u)) => Dataset(Tuple(v, u))
    case _ => Dataset.empty
  }
  
}

object CollectionAggregation {
  def apply(): CollectionAggregation = new CollectionAggregation()
  def apply(ds1: Dataset, ds2: Dataset): Dataset = new CollectionAggregation()(ds1, ds2)
}
