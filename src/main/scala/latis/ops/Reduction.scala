package latis.ops

import latis.dm._

/*
 * Reduce any Tuples of one element to that element.
 */
class Reduction extends IndexedSampleMappingOperation  {
  //TODO: must use IndexedSampleMappingOperation since that is what the WrappedFunction expects, for now
  //TODO: consider reducing Function with one sample to that Sample, maybe too much, just a Tuple, loss of mapping semantics
  /*
   * TODO: need something other than WrappedFunction, munge domain and range types
   * back to idea of subtype of op that munges type and one that doesn't
   * Filter (including Selection) does not
   * maybe we do need Transformation and TransformedFunction
   * and FilteredFunction?
   * WrappedFunction could become MappedFunction that does not use index
   * 
   * if changes model but doesn't touch data, then we can just use applyToSample?
   *   applyToSampleType?
   *   
   * maybe parameterized Functions will save us: Function[D,R](samples: Seq[Sample[D,R]])
   * 
   * what about index support
   *   Filter and Projection
   * could we end up with multiple traits
   */
  
  def applyToSampleType(sample: Sample): Option[Sample] = applyToSample(sample)
  
  /**
   * Apply to the domain and range of the sample and package in a new sample.
   */
  override def applyToSample(sample: Sample): Option[Sample] = sample match {
    case Sample(d,r) => for (d2 <- applyToVariable(d); r2 <- applyToVariable(r)) yield Sample(d2,r2)
  }
  
  /**
   * Recursively apply to all elements.
   * If a Tuple has only one element, reduce it to that element.
   */
  override def applyToTuple(tuple: Tuple): Option[Variable] = {
    //TODO: assumes the tuple does not own the data
    //TODO: don't reduce if tuple is named, preserve namespace; what about other metadata? concat names with "_"?
    val vars = tuple.getVariables.flatMap(applyToVariable(_)) 
    vars.length match {
      case 0 => None
      case 1 => Some(vars.head)
      case _ => Some(Tuple(vars)) //TODO: metadata
    }
  }

  override def applyToFunction(function: Function) = Some(WrappedFunction(function, this))
  
//need to think about this
//  /**
//   * If a Function has only one Sample, reduce to that Sample.
//   */
//  override def applyToFunction(function: Function): Option[Variable] = {
//    //problem if iterable once, so suck the whole thing in //TODO: consider more optimal approach
//    /*
//     * TODO: Iterable Once issues
//     * apply recursively to domain and range
//     */
//    val length = function.getLength
//    if (length == 0) None
//    else if (length == 1) applyToSample(function.getSample(0))
//    else super.applyToFunction(function) //apply to each sample via WrappedFunction
//    
//    //val samples = function.iterator.toList
//    //if (samples.length == 1) applyToVariable(samples.head) else Function(samples) //TODO: Function(f.getMetadata, samples)
//    //TODO: reduce to Tuple instead of Sample? will BE-A tuple but might we want domain/range semantics?
//    //TODO: put Function metadata in Sample?      
//    //TODO: try to reduce every sample of the Function, 
//    //  but only do it if every sample can be reduced in the same way so the range type is the same
//    //  consider tuple range with one element
//  }
  
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