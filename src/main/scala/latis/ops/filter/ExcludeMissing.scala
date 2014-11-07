package latis.ops.filter

import latis.dm.Scalar
import latis.dm.Number
import latis.dm.Variable
import latis.ops.OperationFactory
import latis.dm.Tuple

class ExcludeMissing extends Filter {

  override def applyToScalar(scalar: Scalar): Option[Variable] = {
    if (scalar.isMissing) None
    else Some(scalar)
  }
  
  /**
   * Exclude a Tuple if any element in it is excluded.
   */
  override def applyToTuple(tuple: Tuple): Option[Variable] = {
    val vars = tuple.getVariables.flatMap(applyToVariable(_))
    if (vars.length == tuple.getElementCount) Some(tuple)
    else None
  }
  
  //TODO: Nested Function, exclude entire outer sample
}

object ExcludeMissing extends OperationFactory {
  override def apply(): ExcludeMissing = new ExcludeMissing
}
