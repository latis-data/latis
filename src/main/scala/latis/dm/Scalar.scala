package latis.dm

/**
 * A Variable that represents a single datum.
 */
abstract class Scalar extends Variable {

}

object Scalar {
  
  /**
   * Expose the datum value that the given Scalar represents.
   * For now, return a random int from 0 until 100.
   * TODO: delegate to parent Dataset's Accessor to get data values
   */
  def unapply(scalar: Scalar): Option[_] = Some(scala.util.Random.nextInt(100))
}