package latis.ops.math

import latis.dm.Dataset
import latis.ops.Operation

trait MathOperation extends Operation 

object MathOperation {
  
  def apply(op: (Double,Double) => Double, ds: Dataset) = new BinaryMathOperation(op, ds)  
  
  def apply(op: => (Double) => Double, ds: Dataset) = new UnaryMathOperation(op)
}