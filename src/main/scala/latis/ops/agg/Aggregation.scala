package latis.ops.agg

import latis.dm.Dataset
import latis.ops.Operation

/**
 * Base type for Operations that aggregat (combine) datasets.
 */
trait Aggregation extends Operation {
  def aggregate(dataset: Dataset): Dataset
  
  override def apply(dataset: Dataset) = aggregate(dataset)
  def apply(datasets: Seq[Dataset]) = aggregate(datasets)
  def apply(dataset1: Dataset, dataset2: Dataset) = aggregate(dataset1, dataset2)
  
  def aggregate(datasets: Seq[Dataset]): Dataset = aggregate(Dataset(datasets))
  def aggregate(dataset1: Dataset, dataset2: Dataset): Dataset = aggregate(List(dataset1, dataset2))
}
