package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.ops.agg.TileAggregation
import latis.reader.tsml.ml.Tsml
import latis.ops.agg.TileJoin

/**
 * Delegate to the TileJoin Operation to join Datasets based on their
 * bounding domain sets.
 */
class TileJoinAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {
  /*
   * TODO: JoinAdapter to replace AggregationAdapter.
   * use adapter property to dynamically construct Join.
   */

  def aggregate(left: Dataset, right: Dataset): Dataset = TileJoin(left, right)
  
}