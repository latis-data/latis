package latis.ops

trait Homomorphism[T] {
  def apply(t: T): T
}

trait HomomorphismOption[T] {
  def apply(t: T): Option[T]
}