package latis.ops.filter

import latis.dm.Scalar
import latis.dm.Number
import latis.dm.Variable

class ExcludeMissing extends Filter {

  override def applyToScalar(scalar: Scalar): Option[Variable] = {
    val missingValue = scalar.getMissingValue
    
    scalar match {
      case Number(d) =>
    }
    
    ???
  }
}