package latis.ops

import latis.dm._

class Reduction extends Operation {

  //TODO: abstract up
  def apply(dataset: Dataset): Dataset = {
    //TODO: metadata, add provo
    Dataset(dataset.getVariables.map(op(_)))
  }
  
  private def op(v: Variable): Variable = v match {
    //No way to simplify a Scalar
    case _: Scalar => v
    
    //If a Tuple has only one element, reduce it to that element.
    //TODO support Tuple with data
    //TODO: don't reduce if tuple is named, preserve namespace; what about other metadata?
    //case Tuple(vars) => if (vars.length == 1) vars.head else v
    case Tuple(vars) => if (vars.length == 1) op(vars.head) else v //recursive
    
    //If a Function has only one Sample, reduce to that Sample.
    //TODO: make it only a Tuple?
    case f: Function => {
      //problem if iterable once, so suck the whole thing in //TODO: consider more optimal approach
      val samples = f.iterator.toList
      if (samples.length == 1) op(samples.head) else Function(samples) //TODO: Function(f.getMetadata, samples)
      //TODO: put Function metadata in Sample?
      
      //TODO: try to reduce every sample of the Function, but only do it if every sample can be reduced in the same way so the range type is the same
    }
  }
}

object Reduction {
  
  def reduce(dataset: Dataset): Dataset = {
    (new Reduction)(dataset)
  }
}