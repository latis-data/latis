package latis.dm

class IteratorSet(val it: Iterator[Variable]) extends DomainSet {
  
  def iterator = it
}