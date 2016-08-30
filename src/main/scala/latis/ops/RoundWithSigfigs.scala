package latis.ops

import latis.data.NumberData
import latis.dm._

import scala.math.BigDecimal.RoundingMode

class RoundWithSigfigs(name: String, digits: Option[Int] = None) extends Operation {
  val errorMessage: String = "Sigfigs must be greater than zero"
  
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    val roundTo = (digits, scalar.getMetadata("sigfigs")) match {
      case (Some(i), _) if i.toInt <= 0 => throw new Error(errorMessage)
      case (Some(i), _) => i.toInt
      case (None, Some(sigfigs)) => sigfigs.toInt
      case (_, _) => return Some(scalar)
    }
    (scalar.hasName(name), scalar) match {
      case (true, Real(d)) => {
        val s = scala.math.pow(10, roundTo - scala.math.log10(scala.math.abs(d)).ceil)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(d * s)) / s).toDouble))
      }
      case (true, Integer(l)) => {
        val s = scala.math.pow(10, roundTo - scala.math.log10(scala.math.abs(l)).ceil)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(l * s)) / s).toLong))
      }
      case (_, _) => Some(scalar)
    }
  }
}

object RoundWithSigfigs extends OperationFactory {

  override def apply(args: Seq[String]): RoundWithSigfigs = args match {
    case Seq(n: String, d: String) => new RoundWithSigfigs(n, Some(d.toInt))
  }
  
  def apply(n: String, d: Option[Int] = None ): RoundWithSigfigs = new RoundWithSigfigs(n, d)
}
