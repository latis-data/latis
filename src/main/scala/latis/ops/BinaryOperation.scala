package latis.ops

import latis.dm.Dataset

/**
 * Base trait for operations that act on two Datasets.
 */
trait BinaryOperation { //extends Operation once we turn it into a trait
  
  /**
   * Given two Datasets, return a single Dataset.
   * Abstract apply method to be implemented.
   */
  def apply(dataset1: Dataset, dataset2: Dataset): Dataset
  
  /**
   * Apply the binary operation recursively to a sequence of Datasets
   * starting at the left.
   */
  def apply(datasets: Seq[Dataset]): Dataset = datasets.reduceLeft(apply(_,_))
  
}