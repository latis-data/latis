package latis.ops

import latis.data.NumberData
import latis.dm._
import latis.util.StringUtils

import scala.math.BigDecimal.RoundingMode

class RoundWithSigfigs(name: String, digits: Int) extends Operation {

  override def applyToSample(sample: Sample): Option[Sample] = {
    (applyToVariable(sample.domain), applyToVariable(sample.range)) match {
      case (Some(d), Some(r)) => Some(Sample(d, r))
      case _ => None
    }
  }
  
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    if (scalar.hasName(name)) scalar match {
      case Real(d) => {
        val s = scala.math.pow(10, digits - scala.math.log10(scala.math.abs(d)).ceil)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(d * s)) / s).toDouble))
      }
      case Integer(l) => {
        val s = scala.math.pow(10, digits - scala.math.log10(scala.math.abs(l)).ceil)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(l * s)) / s).toLong))
      }
      case _ => Some(scalar)
    }
    else Some(scalar)
  }
}

object RoundWithSigfigs extends OperationFactory {

  override def apply(args: Seq[String]): RoundWithSigfigs = args match {
    case Seq(n: String, d: String) if StringUtils.isNumeric(d) => apply(n, d.toInt)
    case _ => throw new UnsupportedOperationException("Invalid arguments")
  }
  
  def apply(n: String, d: Int): RoundWithSigfigs = {
    if (d <= 0) {
      throw new Error("Sigfigs must be greater than zero")
    }
    else {
      new RoundWithSigfigs(n, d)
    }
  }
}
