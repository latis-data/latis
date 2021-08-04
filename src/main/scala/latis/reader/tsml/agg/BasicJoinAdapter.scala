package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.ops.agg.BasicJoin
import latis.reader.tsml.ml.Tsml

/**
 * Delegate to the BasicJoin Operation to join Datasets.
 * This generally assumes the same domain set with different sets of variables
 * that need to be combined into the range as a Tuple.
 */
class BasicJoinAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {

  def aggregate(left: Dataset, right: Dataset): Dataset = BasicJoin(left, right)
  
}
