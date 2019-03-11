package latis.ops

import latis.dm.Scalar
import scala.math.ScalaNumericAnyConversions
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text

/**
 * Operation to replace any occurrence of a given value in a Dataset with another.
 * Numeric values will be compared as doubles.
 */
//class ReplaceValueOperation(v1: AnyVal, v2: AnyVal) extends Operation {
class ReplaceValueOperation(v1: Scalar, s2: String) extends Operation {
  /*
   * TODO: allow change of type?
   * But we often get vs from an operation expression (string) so we don't know the intended type.
   * Also issues about converting the model early enough?
   * Keep original type for now.
   */
  //TODO: limit to range vars?

  
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    //TODO: consider exception, type conversion, None?
    //TODO: consider order, v1 data will be converted to type of scalar
    if ((scalar.compare(v1)) == 0) Some(scalar.updatedValue(s2))
    else Some(scalar) //no-op
  }
}

object ReplaceValueOperation extends OperationFactory {

  override def apply(args: Seq[String]): ReplaceValueOperation = ReplaceValueOperation(args(0), args(1))
  
  def apply(v1: Scalar, s2: String): ReplaceValueOperation = new ReplaceValueOperation(v1, s2)
  
  def apply(v1: Any, v2: Any): ReplaceValueOperation = ReplaceValueOperation(Scalar(v1), v2.toString)
}