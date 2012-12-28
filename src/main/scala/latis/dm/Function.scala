package latis.dm

/**
 * A Variable that represents a mapping from an independent (domain) Variable
 * to a dependent (range) Variable.
 */
class Function(val domain: DomainSet, val range: Iterable[Variable]) extends Variable {

//  //Set parentage
//  domain.setParent(this)
//  range.setParent(this)
  
  def iterator() = domain.iterator zip range.iterator
    //getDataset().getAccessor().getIterator(this)
    //FunctionIterator(this)
}

object Function {
  
  def apply(domain: DomainSet, range: Iterable[Variable]) = new Function(domain, range)
  
}