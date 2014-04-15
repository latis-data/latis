package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.PeekIterator

/**
 * Wrapper for a Function that applies a boolean filter to each sample.
 * This Function will represent a subset of the samples of the original.
 * The type of the variables will not change.
 */
class FilteredFunction(function: Function, val filter: Filter) 
  extends SampledFunction(function.getDomain, function.getRange) {
  //NOTE: Filter can only from samples, can't change type: same domain and range
  //TODO: deal with 'length' metadata
  
  override def iterator = new PeekIterator[Sample] {
    lazy val it = function.iterator  //original Function's iterator

//TODO: manage index if this is an IndexFunction, consider a bigger refactoring, see ProjectedFunction...
    
    override def getNext: Sample = {
      if (it.hasNext) {
        val nxt = it.next()
        val myFilter = FilteredFunction.this.filter //avoid name conflict with Iterator's ,filter, method
        myFilter.filterSample(nxt) match {
          case None => getNext //keep trying until we get a valid sample
          case Some(sample) => sample
        }
      } else null //no more valid samples
    }
  }
}

object FilteredFunction {
  
  def apply(function: Function, filter: Filter) = new FilteredFunction(function, filter)
}