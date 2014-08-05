package latis.dm

import latis.data.NumberData

trait Number { this: Scalar =>
  def doubleValue: Double = getNumberData.doubleValue
  def longValue: Long = getNumberData.longValue
  def intValue: Int = getNumberData.intValue
  
  //TODO: unit conversions...
  override def compare(that: Scalar): Int = that match {
    case Number(d) => doubleValue compare d
    case _ => throw new Error("Can't compare " + this + " with " + that)
  }
  
}

object Number {
  //TODO: None if Data isEmpty
  def unapply(num: Number): Option[Double] = Some(num.doubleValue)
}