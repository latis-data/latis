package latis.ops.math

import latis.dm.Dataset
import latis.dm.implicits.doubleToDataset

/**
 * Reduces a Seq of Datasets into one using the operation of a BinaryMathOperation.
 */
class ReductionMathOperation(op: (Double,Double) => Double) extends MathOperation {
  
  def apply(datasets: Seq[Dataset]): Dataset = {
    datasets.reduceLeft((a,b) => MathOperation(op,b)(a))
  }
  
  override def apply(dataset: Dataset): Dataset = dataset //requires multiple datasets

}