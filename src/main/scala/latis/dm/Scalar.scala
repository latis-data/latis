package latis.dm

/**
 * A Variable that represents a single datum of type T.
 */
abstract class Scalar[T](val value: T) extends Variable 

object Scalar {
  
  /**
   * Expose the datum value that the given Scalar represents.
   */
  def unapply[T](s: Scalar[T]): Option[_] = Some(s.value)
}