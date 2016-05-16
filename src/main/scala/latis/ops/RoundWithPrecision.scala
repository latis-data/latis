package latis.ops

import latis.dm.Scalar
import latis.data.NumberData
import latis.dm.Variable
import latis.ops.OperationFactory

import scala.math.BigDecimal.RoundingMode

class RoundWithPrecision(name: String) extends Operation {
 
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    val roundTo: Int = scalar.getMetadata("precision") match {
      case None => return Some(scalar)
      case Some(precision) => precision.toInt
    }
    (scalar.hasName(name), scalar) match {
      case (true, Real(r)) => {
        val s = math.pow(10, roundTo.get)
        Some(Scalar(scalar.getMetadata(), ((math.rint(r * s)) / s).toDouble))
      }
      case (true, Integer(l)) => {
        val s = math.pow(10, roundTo.get)
        Some(Scalar(scalar.getMetadata(), ((math.rint(l * s)) / s).toInt))
      }
      case (_, _) => Some(scalar)
    }
  }
}

object RoundWithPrecision extends OperationFactory {

  override def apply(args: Seq[String]): RoundWithPrecision = args match {
    case Seq(n: String) => new RoundWithPrecision(n)
  }
  
  def apply(n: String): RoundWithPrecision = new RoundWithPrecision(n)
}
