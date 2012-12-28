package latis.dm

class Integer(value: Long) extends Scalar[Long](value)

object Integer {
  def apply(value: Long) = new Integer(value)
}
