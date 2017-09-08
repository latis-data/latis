package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.ops.agg.TileAggregation
import latis.reader.tsml.ml.Tsml
import latis.ops.agg.Join

/**
 * Delegate to the Join Operation to join Datasets.
 * This generally assumes the same domina set with different sets of variables
 * that need to be combined into the range as a Tuple.
 */
class JoinAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {

  def aggregate(left: Dataset, right: Dataset): Dataset = Join(left, right)
  
}