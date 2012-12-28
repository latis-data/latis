package latis.dm

class VariableSeq(val it: Iterator[Variable]) extends Iterable[Variable] {
  //TODO: extend Seq[Variable]?
  
  def iterator = it
}

object VariableSeq {
  
  def apply(iterator: Iterator[Variable]) = new VariableSeq(iterator)
}