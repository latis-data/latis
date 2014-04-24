package latis.ops

import latis.dm._

class Reduction extends Operation  {
  //TODO: changing types, can't use default WrappedFunction?

  /**
   * If a Tuple has only one element, reduce it to that element.
   */
  override def applyToTuple(tuple: Tuple) = {
    //TODO: deal with data
    //TODO: don't reduce if tuple is named, preserve namespace; what about other metadata? concat names with "_"?
    val vars = tuple.getVariables
    if (vars.length == 1) applyToVariable(vars.head) else Some(tuple) //recursive
  }

  //SampleHomomorphism method
  def apply(sample: Sample): Option[Sample] = applyToSample(sample)
  
  override def applyToSample(sample: Sample): Option[Sample] = sample match {
    case Sample(d,r) => for (d2 <- applyToVariable(d); r2 <- applyToVariable(r)) yield Sample(d2,r2)
  }

  /**
   * If a Function has only one Sample, reduce to that Sample.
   */
  override def applyToFunction(function: Function): Option[Variable] = {
    //problem if iterable once, so suck the whole thing in //TODO: consider more optimal approach
    /*
     * TODO: Iterable Once issues
     * apply recursively to domain and range
     */
    val length = function.getLength
    if (length == 0) None
    else if (length == 1) applyToSample(function.getSample(0))
    else super.applyToFunction(function) //apply to each sample via WrappedFunction
    
    //val samples = function.iterator.toList
    //if (samples.length == 1) applyToVariable(samples.head) else Function(samples) //TODO: Function(f.getMetadata, samples)
    //TODO: reduce to Tuple instead of Sample? will BE-A tuple but might we want domain/range semantics?
    //TODO: put Function metadata in Sample?      
    //TODO: try to reduce every sample of the Function, 
    //  but only do it if every sample can be reduced in the same way so the range type is the same
    //  consider tuple range with one element
  }
  
//  private def op(v: Variable): Variable = v match {
//    //No way to simplify a Scalar
//    case _: Scalar => v
//    
//    //If a Tuple has only one element, reduce it to that element.
//    //TODO support Tuple with data
//    //TODO: don't reduce if tuple is named, preserve namespace; what about other metadata?
//    //case Tuple(vars) => if (vars.length == 1) vars.head else v
//    case Tuple(vars) => if (vars.length == 1) op(vars.head) else v //recursive
//    
//    //If a Function has only one Sample, reduce to that Sample.
//    //TODO: make it only a Tuple?
//    case f: Function => {
//      //problem if iterable once, so suck the whole thing in //TODO: consider more optimal approach
//      val samples = f.iterator.toList
//      if (samples.length == 1) op(samples.head) else Function(samples) //TODO: Function(f.getMetadata, samples)
//      //TODO: put Function metadata in Sample?
//      
//      //TODO: try to reduce every sample of the Function, but only do it if every sample can be reduced in the same way so the range type is the same
//    }
//  }
}

object Reduction {
  //TODO: extend OptionFactory, allow in server request?
  
  def reduce(dataset: Dataset): Dataset = {
    (new Reduction)(dataset)
  }
}