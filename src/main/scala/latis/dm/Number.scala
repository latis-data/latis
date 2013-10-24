package latis.dm

import latis.data.NumberData

trait Number { this: Scalar =>
  def doubleValue: Double = getNumberData.doubleValue
  def longValue: Long = getNumberData.longValue
  def intValue: Int = getNumberData.intValue
}

object Number {
  def unapply(num: Number): Option[Double] = Some(num.doubleValue)
}