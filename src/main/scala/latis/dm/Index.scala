package latis.dm

class Index(value: Int) extends Scalar[Int](value)

object Index {
  def apply(value: Int) = new Index(value)
}