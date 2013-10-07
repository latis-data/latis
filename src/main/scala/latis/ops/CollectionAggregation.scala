package latis.ops

import latis.dm.Dataset

class CollectionAggregation extends Aggregation {

  
  def aggregate(dataset: Dataset): Dataset = {
    //TODO: replace child Datasets with instances of Tuple
    dataset
  }
}

object CollectionAggregation {
  def apply() = new CollectionAggregation
}