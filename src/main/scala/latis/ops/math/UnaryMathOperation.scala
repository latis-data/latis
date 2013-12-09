package latis.ops.math

import latis.dm._

class UnaryMathOperation(op: => (Double) => Double) extends MathOperation {

  override def transformScalar(scalar: Scalar): Variable = scalar match {
    case Number(d) => Real(op(d))
    case _: Text => Real(Double.NaN)
  }
}