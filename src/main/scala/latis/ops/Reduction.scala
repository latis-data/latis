package latis.ops

import latis.dm._

class Reduction extends Operation {

  //TODO: abstract up
  def apply(dataset: Dataset): Dataset = {
    //TODO: metadata, add provo
    Dataset(dataset.getVariables.map(op(_)))
  }
  
  private def op(v: Variable): Variable = v match {
    case _: Scalar => v
    
    //TODO support Tuple with data
    //TODO: don't reduce if tuple is named, preserve namespace; what about other metadata?
    case Tuple(vars) => if (vars.length == 1) vars.head else v
    
    case f: Function => {
      //problem if iterable once, so suck the whole thing in //TODO: consider more optimal approach
      val samples = f.iterator.toList
      if (samples.length == 1) samples.head else Function(samples) //TODO: Function(f.getMetadata, samples)
      //TODO: put Function metadata in Sample?
    }
  }
}

object Reduction {
  
  def reduce(dataset: Dataset): Dataset = {
    (new Reduction)(dataset)
  }
}