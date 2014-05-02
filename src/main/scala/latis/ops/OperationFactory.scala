package latis.ops

/**
 * Trait for companion objects of Operations to extends so they can be constructed
 * from name (and potentially arguments) via the Operation factory ('apply') methods.
 */
trait OperationFactory {

  /**
   * Override for Operations that accept arguments.
   * Throw exception for Operations that do not accept arguments.
   */
  def apply(args: Seq[String]): Operation = throw new UnsupportedOperationException("Operation does not accept arguments")
  
  /**
   * Override for Operations that do not accept arguments.
   * Throw exception for Operations that require arguments.
   */
  def apply(): Operation = throw new UnsupportedOperationException("Operation requires arguments")
}