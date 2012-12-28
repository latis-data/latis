package latis.dm

/**
 * A Variable that represents a single datum of type T.
 */
abstract class Scalar[T] extends Variable 

object Scalar {
  
  /**
   * Expose the datum value that the given Scalar represents.
   * Delegate to parent Dataset's Accessor to get data values.
   */
  //def unapply[T](v: Scalar[T]): Option[_] = v.getDataset().getAccessor().getValue(v) 
}