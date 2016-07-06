package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Tuple

/**
 * Join Datasets with top level Functions by concatenating the Function Samples.
 */
class TileJoin extends Join { 
  //Intended to replace TileAggregation
  
  def apply(ds1: Dataset, ds2: Dataset) = {
    (ds1, ds2) match {
      case (Dataset(v1), Dataset(v2)) => (v1, v2) match {
        case (f1 @ Function(it1), f2 @ Function(it2)) => {
          //TODO: assumes both functions have the same type, units
          Dataset(Function(f1.getDomain, f1.getRange, it1 ++ it2))
        }
        case _ => throw new UnsupportedOperationException("TileJoin expects a Function in each of the Datasets it joins.")
      }
      case (Dataset(_), _) => ds1 //ds2 empty
      case (_, Dataset(_)) => ds2 //ds2 empty
      case _ => Dataset.empty //both datasets empty
    }
  }
  
}

object TileJoin {
  def apply() = new TileJoin()
  def apply(ds1: Dataset, ds2: Dataset) = new TileJoin()(ds1, ds2)
}