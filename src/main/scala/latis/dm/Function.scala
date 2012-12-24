package latis.dm

/**
 * A Variable that represents a mapping from an independent (domain) Variable
 * to a dependent (range) Variable.
 */
class Function(val domain: Variable, val range: Variable) extends Variable {

  //Set parentage
  domain.setParent(this)
  range.setParent(this)
  
  def iterator() = getDataset().getAccessor().getIterator(this)
    //FunctionIterator(this)
}

object Function {
  
  def apply(domain: Variable, range: Variable) = new Function(domain, range)
  
//  /**
//   * Expose a Functions domain and range Variables as a pair.
//   */
//  def unapply(f: Function): Option[(Variable, Variable)] = Some(f.domain, f.range)
}