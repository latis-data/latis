package latis.ops

import latis.dm.Scalar
import scala.math.ScalaNumericAnyConversions
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text

/**
 * Operation to replace any occurrence of a given value in a Dataset with another.
 */
class ReplaceValueOperation(v1: AnyVal, v2: AnyVal) extends Operation {
  //TODO: allow change of type?

  override def applyToScalar(scalar: Scalar): Option[Scalar] = scalar match {
    case r1: Real if (r1.compare(Real(v1)) == 0) => Some(Real(scalar.getMetadata, v2))
    case i1: Integer if (i1.compare(Integer(v1)) == 0) => Some(Integer(scalar.getMetadata, v2))
    case t1: Text if (t1.compare(Text(v1)) == 0) => Some(Text(scalar.getMetadata, v2))
    case _ => Some(scalar)
  }
  
}


object ReplaceValueOperation {
  //TODO: extend OperationFactory so we can construct from expression: replace(v1,v2)
  
  def apply(v1: AnyVal, v2: AnyVal) = new ReplaceValueOperation(v1, v2)
}