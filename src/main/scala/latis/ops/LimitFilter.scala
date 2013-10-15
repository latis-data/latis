package latis.ops

import latis.dm._

class LimitFilter(args: Seq[String]) extends Operation {
  
  val limit = args.head.toInt
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.variables.flatMap(filter(_)))
    //TODO: provenance metadata
  }
  
  def filter(variable: Variable): Option[Variable] = variable match {
    //this should only do top level variables since filter does not recurse
    case s: Scalar => Some(s)
    case t: Tuple => Some(t) //TODO: limit number of elements?
    case f: Function => Some(Function(f.domain, f.range, f.iterator.take(limit))) //TODO: metadata
  }

}

object LimitFilter {
  def apply(args: Seq[String]): LimitFilter = new LimitFilter(args)
  def apply(limit: Int): LimitFilter = new LimitFilter(Seq(limit.toString))
}
