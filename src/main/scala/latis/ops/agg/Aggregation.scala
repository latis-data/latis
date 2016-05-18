package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Function
import latis.ops.Operation
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.dm.Variable

/**
 * Base type for Operations that aggregate (combine) datasets.
 */
trait Aggregation extends Operation {
  //See LATIS-325 about using relational algebra "join" terminology.
  
  def aggregate(ds1: Dataset, ds2: Dataset): Dataset
      
  override def apply(dataset: Dataset) = dataset match {
    //support for old usage
    case Dataset(Tuple(Seq(f1: Function, f2: Function))) => 
      aggregate(Dataset(f1), Dataset(f2)) match {
        case Dataset(v) => Dataset(v, dataset.getMetadata)
        case _ => Dataset.empty
      }
    case _ => throw new UnsupportedOperationException("An Aggregation can only be applied to two or more Datasets.")
  }
  
  def apply(datasets: Seq[Dataset]) = datasets.reduceLeft(aggregate(_,_))
  def apply(dataset1: Dataset, dataset2: Dataset, md: Metadata = EmptyMetadata) = aggregate(dataset1, dataset2) match {
    case Dataset(v: Variable) => Dataset(v, md)
    case _ => throw new UnsupportedOperationException(s"Failed to aggregate datasets '$dataset1' and '$dataset2'")
  }
}
