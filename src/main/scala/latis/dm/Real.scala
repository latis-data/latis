package latis.dm

class Real(value: Double) extends Scalar[Double](value)

object Real {
  def apply(value: Double) = new Real(value)
}