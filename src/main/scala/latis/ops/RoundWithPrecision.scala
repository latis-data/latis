package latis.ops

import latis.data.NumberData
import latis.dm._
import latis.util.StringUtils

import scala.math.BigDecimal.RoundingMode

class RoundWithPrecision(name: String, digits: Int) extends Operation {

  override def applyToSample(sample: Sample): Option[Sample] = {
    (applyToVariable(sample.domain), applyToVariable(sample.range)) match {
      case (Some(d), Some(r)) => Some(Sample(d, r))
      //if applying to either variable fails, invalidate teh whole sample (?)
      case _ => None
    }
  }

  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    if (scalar.hasName(name)) scalar match {
      case Real(r) => {
        val s = scala.math.pow(10, digits)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(r * s)) / s).toDouble))
      }
      case Integer(l) => {
        val s = scala.math.pow(10, digits)
        Some(Scalar(scalar.getMetadata(), ((scala.math.rint(l * s)) / s).toLong))
      }
      case _ => Some(scalar)
    }
    else Some(scalar)
  }
}

object RoundWithPrecision extends OperationFactory {

  override def apply(args: Seq[String]): RoundWithPrecision = args match {
    case Seq(n: String, d: String) if StringUtils.isNumeric(d) => apply(n, d.toInt)
    case _ => throw new UnsupportedOperationException("Invalid arguments")
  }
  
  def apply(n: String, d: Int): RoundWithPrecision = {
    if (d <= 0) {
      throw new Error("Precision must be a postive integer")
    }
    else {
      new RoundWithPrecision(n, d)
    }
  }
}
