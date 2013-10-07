package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.reader.tsml.Tsml
import latis.ops.CollectionAggregation

class CollectionAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {

  def aggregate(dataset: Dataset): Dataset = {
    val agg = CollectionAggregation()
    agg(dataset)
  }
}