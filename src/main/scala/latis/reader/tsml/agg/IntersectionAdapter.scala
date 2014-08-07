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

  /*
   * TODO: revisit dataset construction process
   * aggregate happens before calling makeDataset, during first pass
   * this is the origDataset that feeds makedataset, but it is already made (i.e. data hooked in)
   * should 1st pass only deal with metadata?
   * does aggregation actually read data? doubt it, just wraps iterators
   * 
   */
  /**
   * Bypass the 2nd construction pass since the aggregation has already made the new Dataset with data hooked in.
   */
  override protected def makeDataset(ds: Dataset) = ds


}