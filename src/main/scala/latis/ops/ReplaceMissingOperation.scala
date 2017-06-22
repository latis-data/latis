package latis.ops

import latis.dm.Scalar
import scala.math.ScalaNumericAnyConversions
import latis.dm.Integer
import latis.dm.Real

/**
 * Operation to replace any missing value in a Dataset with another.
 */
class ReplaceMissingOperation(value: ScalaNumericAnyConversions) extends Operation {
  //TODO: ExcludeMissing, filter
  //TODO: apply to text
  //TODO: update metadata missing_value

  override def applyToScalar(scalar: Scalar): Option[Scalar] = scalar match {
    //TODO: use scalar.copy(data = ...) (LATIS-428)
    case r: Real if (r.isMissing) => Some(Real(r.getMetadata, value.doubleValue))
    case i: Integer if (i.isMissing) => Some(Integer(i.getMetadata, value.longValue))
    case _ => Some(scalar)
  }

}


object ReplaceMissingOperation extends OperationFactory {
  
  override def apply(args: Seq[String]): ReplaceMissingOperation = {
    if (args.length != 1) throw new UnsupportedOperationException("The ReplaceMissingOperation requires one argument.")
    val v: ScalaNumericAnyConversions = args.headOption match  {
      case Some(s) => {
        if (s == "NaN") Double.NaN
        else s.toDouble
        //TODO: need to know what type to convert string to
      }
      case None => ???
    }
    
    ReplaceMissingOperation(v)
  }
  
  def apply(value: ScalaNumericAnyConversions): ReplaceMissingOperation = new ReplaceMissingOperation(value)
}