package latis.dm

trait Number {
  def toDouble: Double
}

object Number {
  
  def unapply(num: Number): Option[Double] = Some(num.toDouble)
}