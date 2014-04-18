package latis.ops

trait Isomorphism[T] extends Homomorphism[T] {
  def unapply(t: T): Option[T]
}

class Triple extends Isomorphism[Double] {
  def apply(d: Double) = d*3
  def unapply(d: Double) = Some(d/3)
}

object Triple extends App {
  val trip = new Triple
  val d = 2.0
  val d2 = trip(d)
  println(d2)
  val d3 = d2 match {
    case trip(d) => d
  }
  println(d3)
  assert(d == d3)
}