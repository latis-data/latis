package latis.ops

import latis.data.NumberData
import latis.dm._

import scala.math.BigDecimal.RoundingMode

class RoundWithPrecision(name: String, digits: Option[Int] = None) extends Operation {
  val errorMessage: String = "Precision must be a postive integer"
 
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    val roundTo: Int = (digits, scalar.getMetadata("precision")) match {
      case (Some(i), _) if i.toInt < 0 => throw new Error(errorMessage)
      case (Some(i), _) => i.toInt
      case (None, Some(precision)) => precision.toInt
      case (_, _) => return Some(scalar)
    }
    (scalar.hasName(name), scalar) match {
      case (true, Real(r)) => {
        val s = scala.math.pow(10, roundTo)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(r * s)) / s).toDouble))
      }
      case (true, Integer(l)) => {
        val s = scala.math.pow(10, roundTo)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(l * s)) / s).toLong))
      }
      case (_, _) => Some(scalar)
    }
  }
}

object RoundWithPrecision extends OperationFactory {

  override def apply(args: Seq[String]): RoundWithPrecision = args match {
    case Seq(n: String, d: String) => new RoundWithPrecision(n, Some(d.toInt))
  }
  
  def apply(n: String, d: Option[Int] = None ): RoundWithPrecision = new RoundWithPrecision(n, d)
}
