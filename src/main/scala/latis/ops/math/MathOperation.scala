package latis.ops.math

import latis.dm.Dataset
import latis.ops.Operation

trait MathOperation extends Operation 

object MathOperation {
  
  def apply(op: (Double,Double) => Double, ds: Dataset): BinaryMathOperation = new BinaryMathOperation(op, ds)  
  
  def apply(op: => (Double) => Double): UnaryMathOperation = new UnaryMathOperation(op)
  
  def apply(op: (Double,Double) => Double): ReductionMathOperation = new ReductionMathOperation(op)
}