package latis.ops

import latis.dm._

class LastFilter extends Operation {
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.getVariables.flatMap(filter(_)))
    //TODO: provenance metadata
  }
  
  def filter(variable: Variable): Option[Variable] = variable match {
    //this should only do top level variables since filter does not recurse
    case s: Scalar => Some(s)
    case t: Tuple => Some(t) //TODO: last element?
    case f: Function => Some(f.getLastSample)
  }

}

object LastFilter {
  def apply() = new LastFilter
  def apply(dataset: Dataset): Dataset = LastFilter()(dataset)
}