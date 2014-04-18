package latis.ops.math

import latis.dm._
import latis.ops.xform._
import latis.ops.Operation
//import latis.dm.implicits._

trait MathOperation extends Operation 

object MathOperation {
  
  def apply(op: (Double,Double) => Double, ds: Dataset) = new BinaryMathOperation(op, ds)
  //def apply(v: Variable, op: BinOp) = new PostBinaryOp(op, v)
  //def apply(op: BinOp, v: Variable) = new PreBinaryOp(op, v)
  
  
  def apply(op: => (Double) => Double, ds: Dataset) = new UnaryMathOperation(op)
}