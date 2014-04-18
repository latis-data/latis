package latis.ops.math

import latis.dm._

class UnaryMathOperation(op: => (Double) => Double) extends MathOperation {

  override def applyToScalar(scalar: Scalar): Option[Real] = scalar match {
    case Number(d) => Some(Real(scalar.getMetadata, op(d))) //TODO: transform metadata
    case _: Text => Some(Real(Double.NaN))
  }
}