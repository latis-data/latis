package latis.dm

/**
 * A Variable that represents a mapping from an independent (domain) Variable
 * to a dependent (range) Variable.
 */
class Function(val domain: Variable, val range: Variable) extends Variable {

}

object Function {
  
  /**
   * Expose a Functions domain and range Variables as a pair.
   */
  def unapply(f: Function) = (f.domain, f.range)
}