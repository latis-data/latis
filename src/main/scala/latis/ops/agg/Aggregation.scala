package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Function
import latis.ops.Operation
import latis.dm.Tuple

/**
 * Base type for Operations that aggregate (combine) datasets.
 */
trait Aggregation extends Operation {
  //See LATIS-325 about using relational algebra "join" terminology.
  
  def aggregate(ds1: Dataset, ds2: Dataset): Dataset
      
  override def apply(dataset: Dataset) = dataset match {
    //support for old usage
    case Dataset(Tuple(Seq(f1: Function, f2: Function))) => 
      Dataset(aggregate(Dataset(f1), Dataset(f2)).unwrap, dataset.getMetadata)
    case _ => throw new UnsupportedOperationException("An Aggregation can only be applied to two or more Datasets.")
  }
  
  def apply(datasets: Seq[Dataset]) = datasets.reduceLeft(aggregate(_,_))
  def apply(dataset1: Dataset, dataset2: Dataset) = aggregate(dataset1, dataset2)
}
