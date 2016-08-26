package latis.dm

import latis.data.NumberData
import scala.math.BigDecimal.RoundingMode

trait Number { this: Scalar =>
  def doubleValue: Double = getNumberData.doubleValue.toDouble
  def longValue: Long = getNumberData.longValue.toLong
  def intValue: Int = getNumberData.intValue.toInt
}

object Number {
  //TODO: None if Data isEmpty
  def unapply(num: Number): Option[Double] = Some(num.doubleValue)
}
