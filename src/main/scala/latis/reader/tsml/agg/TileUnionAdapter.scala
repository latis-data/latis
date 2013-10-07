package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.reader.tsml.ml.Tsml
import latis.ops.TileAggregation

class TileUnionAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {

  def aggregate(dataset: Dataset): Dataset = {
    val agg = TileAggregation()
    agg(dataset)
  }
}