package latis.ops

import latis.dm.Scalar
import latis.data.NumberData
import latis.dm.Variable

import scala.math.BigDecimal.RoundingMode

class RoundWithSigfigs(name: String) extends Operation {
  
  override def applyToScalar(scalar: Scalar): Option[Scalar] = Some(scalar) {
    val roundTo = scalar.getMetadata("sigfigs") match {
      case None => return Some(scalar)
      case Some(precision) => precision.toInt
    }
    (scalar.hasName(name), scalar) match {
      case (true, Real(d)) => {
        val s = math.pow(10, roundTo.get - math.log10(math.abs(d)).ceil)
        Some(Scalar(scalar.getMetadata(), ((math/rint(d * s)) / s).toDouble))
      }
      case (true, Integer(l)) => {
        val s = math.pow(10, roundTo.get - math.log10(math.abs(l)).ceil)
        Some(Scalar(scalar.getMetadata(), ((math/rint(l * s)) / s).toLong))
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
