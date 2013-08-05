package latis.dm

trait Number {
  def doubleValue: Double
  def compare(that: Double): Int = doubleValue.compareTo(that)
  def compare(that: String): Int = compare(that.toDouble)
  //TODO: add epsilon for equality?
}

object Number {
  def unapply(num: Number): Option[Double] = Some(num.doubleValue)
}