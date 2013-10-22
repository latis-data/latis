package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.NextIterator

/**
 * Wrapper for a Function that applies a boolean filter to each sample.
 * This Function will represent a subset of the samples of the original.
 * The type of the variables will not change.
 */
class FilteredFunction(function: Function, val selection: Selection) extends Variable with Function {
  //NOTE: can't change type: same domain and range, can't be used for projection
  //TODO: operations: Seq[Operation]? CompositeOperation?
  //TODO: deal with metadata? only if there is a "length", provenance belongs to parent Dataset

  //TODO: override metadata, data? 
  
  override def iterator = new NextIterator[Sample] {
    lazy val it = function.iterator  //original Function's iterator
    
/*
 * TODO: should iterator always return Sample?
 * does it matter? just let pattern matching handle filtering?
 * does it make sense to iterate on other var types?
 *   tuple might iter over members, or implicit IndexFunction if it has data?
 * what about for general transform, could be diff type
 *   but will xformed data ever have no domain?
 *   maybe if a function is reduced to a single value
 *   e.g. integrated
 *   but would still have a relevant "nominal" domain value
 *   domain could always be an index
 * are there any other cases of iterating over Variables that are not Function samples?
 */
    
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