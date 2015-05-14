package latis.dm

import latis.data.NumberData
import scala.math.BigDecimal.RoundingMode

trait Number { this: Scalar =>
  def doubleValue: Double = applyRounding(getNumberData.doubleValue).toDouble
  def longValue: Long = applyRounding(getNumberData.longValue).toLong
  def intValue: Int = applyRounding(getNumberData.intValue).toInt
  
  def applyRounding(d: Double): Double = (this.getMetadata("sigfigs"), this.getMetadata("precision")) match {
    case (None, None) => d
    case (Some(sf), None) => {
      val s = math.pow(10, sf.toInt - math.log10(math.abs(d)).ceil)
      (math.rint(d * s)) / s
    }
    case (None, Some(p)) => {
      val s = math.pow(10,p.toInt)
      (math.rint(d * s)) / s
    }
    case (Some(sf), Some(p)) => {
      val s1 = math.pow(10, sf.toInt - math.log10(math.abs(d)).ceil)  
      val s2 = math.pow(10,p.toInt)
      val s = math.min(s1,s2)
      (math.rint(d * s)) / s
    }
  }
}

object Number {
  //TODO: None if Data isEmpty
  def unapply(num: Number): Option[Double] = Some(num.doubleValue)
}