package latis.dm

abstract class DomainSet extends Iterable[Variable] {
  //TODO: extend SortedSet
}

object DomainSet {
  
  def apply(iterator: Iterator[Variable]) = new IteratorSet(iterator)
}