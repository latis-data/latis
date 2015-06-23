package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Function
import latis.ops.Operation
import latis.dm.Tuple

/**
 * Base type for Operations that aggregate (combine) datasets.
 */
trait Aggregation extends Operation {
  /*
   * TODO: rename to join?
   * aggregate in the sense of combining a seq of datasets into one is consistent with sum, mean,... aggregations
   * try to be consistent with relational algebra and FP
   * 
   */
  
  def aggregate(ds1: Dataset, ds2: Dataset): Dataset
      
  override def apply(dataset: Dataset) = dataset match { 
    case Dataset(Tuple(Seq(f1: Function, f2: Function))) => Dataset(aggregate(Dataset(f1), Dataset(f2)).unwrap, dataset.getMetadata)
    case _ => throw new UnsupportedOperationException("An Aggregation can only be applied to two or more Datasets.")
  }
  
  def apply(datasets: Seq[Dataset]) = datasets.reduceLeft(aggregate(_,_))
  def apply(dataset1: Dataset, dataset2: Dataset) = aggregate(dataset1, dataset2)
}
