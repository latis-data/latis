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
   * TODO: Datasets no longer can contain datasets
   * probably should make this a binary operation to fit best with the paradigm
   * 
   * rename to join?
   * aggregate in the sense of combining a seq of datasets into one is consistent with sum, mean,... aggregations
   * try to be consistent with relational algebra and FP
   * 
   */
  
  //tmp hack
  //dataset is expected to contain a tuple with the top level variables from each dataset to be aggregated
  def getFunctions(dataset: Dataset) = {
    val tup = dataset.unwrap.asInstanceOf[Tuple]
    tup.getVariables.map(_.asInstanceOf[Function])
  }
    
  def aggregate(dataset: Dataset): Dataset
  
  override def apply(dataset: Dataset) = aggregate(dataset)
//  def apply(datasets: Seq[Dataset]) = aggregate(datasets)
//  def apply(dataset1: Dataset, dataset2: Dataset) = aggregate(dataset1, dataset2)
//  
//  def aggregate(datasets: Seq[Dataset]): Dataset = aggregate(Dataset(datasets))
//  def aggregate(dataset1: Dataset, dataset2: Dataset): Dataset = aggregate(List(dataset1, dataset2))
}
