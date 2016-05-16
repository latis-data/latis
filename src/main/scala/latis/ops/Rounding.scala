package latis.ops

import latis.dm.Scalar
import latis.data.NumberData
import latis.dm.Variable

import scala.math.BigDecimal.RoundingMode

class Rounding extends Operation {
  override def applyToScalar(scalar: Scalar): Scalar = Some(scalar)
}

object Rounding extends OperationFactory {
  
  override def apply(): Rounding  = new Rounding()

  override def apply(set: Dataset): Rounding = new Rounding(dataset)
}
