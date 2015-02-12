package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.dm.Function
import latis.reader.tsml.ml.Tsml
import latis.ops.agg.Intersection

/**
 * Delegate to the Intersection Aggregation Operation to join Datasets 
 * by keeping only samples that have common domain values and merging
 * all range parameters.
 */
class IntersectionAdapter(tsml: Tsml) extends AggregationAdapter(tsml) {

  def aggregate(dataset: Dataset): Dataset = {
    val agg = Intersection()
    agg(dataset)
  }

}