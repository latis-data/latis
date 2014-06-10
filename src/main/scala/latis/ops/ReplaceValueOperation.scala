package latis.ops

import latis.dm.Scalar
import scala.math.ScalaNumericAnyConversions
import latis.dm.Integer
import latis.dm.Real

/**
 * Operation to replace any occurrence of a given numeric value in a Dataset with another.
 */
class ReplaceValueOperation(v1: ScalaNumericAnyConversions, v2: ScalaNumericAnyConversions) extends Operation {
  //TODO: name should be a noun?
  //TODO: support text? DataValue?
  //TODO: allow change of type?

  override def applyToScalar(scalar: Scalar): Option[Scalar] = scalar match {
    case Real(d)    if (bothNaN(d, v1) || d == v1.doubleValue) => Some(Real(scalar.getMetadata, v2.doubleValue))
    case Integer(l) if (l == v1.longValue) => Some(Integer(scalar.getMetadata, v2.longValue))
  }
  
  private def bothNaN(v1: Double, v2: ScalaNumericAnyConversions) = v1.isNaN && v2.doubleValue.isNaN
}


object ReplaceValueOperation {
  //TODO: extend OperationFactory so we can construct from expression: replace(v1,v2)
  
  def apply(v1: ScalaNumericAnyConversions, v2: ScalaNumericAnyConversions) = new ReplaceValueOperation(v1, v2)
}