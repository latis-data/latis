package latis.ops

import latis.data.NumberData
import latis.dm._

import scala.math.BigDecimal.RoundingMode

class RoundWithSigfigs(name: String) extends Operation {
  
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    val roundTo = scalar.getMetadata("sigfigs") match {
      case None => return Some(scalar)
      case Some(precision) => precision.toInt
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
    }
  }
}

object RoundWithSigfigs extends OperationFactory {

  override def apply(args: Seq[String]): RoundWithSigfigs = args match {
    case Seq(n: String) => new RoundWithSigfigs(n)
  }
  
  def apply(n: String): RoundWithSigfigs = new RoundWithSigfigs(n)
}
