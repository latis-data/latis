package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.ops.agg.TileAggregation
import latis.reader.tsml.ml.Tsml

/**
 * Delegate to the TileAggregation Operation to join Datasets based on their
 * bounding domain sets.
 */
class TileUnionAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {

  def aggregate(left: Dataset, right: Dataset): Dataset = {
    val agg = TileAggregation()
    agg(left, right)
  }
}