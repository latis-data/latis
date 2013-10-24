package latis.ops

import latis.dm._

class FirstFilter extends Operation {
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.getVariables.flatMap(filter(_)))
    //TODO: provenance metadata
  }
  
  def filter(variable: Variable): Option[Variable] = variable match {
    //this should only do top level variables since filter does not recurse
    case s: Scalar => Some(s)
    case t: Tuple => Some(t) //TODO: first element?
    case f: Function => Some(f.getFirstSample)
    //TODO: consider nested functions, 
  }

}

object FirstFilter {
  def apply(): FirstFilter = new FirstFilter
  
  def apply(dataset: Dataset): Dataset = FirstFilter()(dataset)
}
