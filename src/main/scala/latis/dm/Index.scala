package latis.dm

class Index(val value: Int) extends Scalar[Int]

object Index {
  def apply(value: Int) = new Index(value)
  
  def unapply(index: Index): Option[Int] = Some(index.value)
}