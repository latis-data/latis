package latis.dm

import latis.data.NumberData

trait Number { this: Scalar[_] =>
  def doubleValue: Double = getNumberData.doubleValue
  def compare(that: Double): Int = doubleValue.compareTo(that)
  def compare(that: String): Int = compare(that.toDouble)
  //TODO: add epsilon for equality?
}

object Number {
  def unapply(num: Number): Option[Double] = Some(num.doubleValue)
}