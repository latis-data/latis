package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.PeekIterator

/**
 * Wrapper for a Function that applies a boolean filter to each sample.
 * This Function will represent a subset of the samples of the original.
 * The type of the variables will not change.
 */
class FilteredFunction(function: Function, val selection: Selection) 
  extends SampledFunction(function.getDomain, function.getRange) {
  //NOTE: can't change type: same domain and range, can't be used for projection
  //TODO: operations: Seq[Operation]? CompositeOperation?
  //TODO: deal with metadata? only if there is a "length", provenance belongs to parent Dataset

  //TODO: override metadata, data? 
  
  override def iterator = new PeekIterator[Sample] {
    lazy val it = function.iterator  //original Function's iterator

//TODO: manage index if this is an IndexFunction, consider a bigger refactoring, see ProjectedFunction...
    
    override def getNext: Sample = {
      if (it.hasNext) {
        val nxt = it.next()
        selection.filter(nxt) match {
          //TODO: could just use "filter" and treat it as Tuple, but need to return Sample here
          //  let next be any Variable?
          //assume that we are getting a 2-tuple, for now
          case None => getNext //keep trying until we get a valid sample
          case Some(tup: Tuple) => Sample(tup(0), tup(1)) //TODO: assert size = 2
          case _ => throw new Error("Filtered sample should be a 2-Tuple.")
        }
      } else null //no more valid samples
    }
  }
}

object FilteredFunction {
  
  def apply(function: Function, selection: Selection) = new FilteredFunction(function, selection)
}