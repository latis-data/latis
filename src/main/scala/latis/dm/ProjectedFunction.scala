package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.PeekIterator

/**
 * Wrapper for a Function that applies a "projection" to each sample.
 * The resulting Function will include only the Variables named in the Projection.
 */
class ProjectedFunction(function: Function, val projection: Projection) 
  extends SampledFunction(null, null) {
//pass null domain and range since we override them here
  
  //TODO: return data in projected order
  
  //Keep current sample index.
  private var index = -1
  
  override def getDomain: Variable = _domain
  override def getRange: Variable = _range
  
  //Get the projected domain and range 'templates' (don't necessarily have data)
  lazy val (_domain, _range) = projectSample(Sample(function.getDomain, function.getRange)) match {
    case Some(sample) => (sample.domain, sample.range)
    case None => ??? //TODO: nothing projected, bug?
  }
  
  
  override def iterator = new PeekIterator[Sample] {
    lazy val it = function.iterator  //original Function's iterator
    
    override def getNext: Sample = {
      index += 1
      if (it.hasNext) {
        projectSample(it.next) match {
          case Some(sample) => sample
          case None => throw new Error("Nothing was projected.")
        }
      } else null //no more valid samples
    }
  }
  
  /*
   * If the domain is not projected, replace with Index.
   * If the range is not projected, make domain the range of a function of index.
   */
  def projectSample(sample: Sample): Option[Sample] = {
    val pd = projection.project(sample.domain)
    val pr = projection.project(sample.range)
    (pd,pr) match {
      case (Some(d), Some(r)) => Some(Sample(d,r))
      case (None, Some(r))    => Some(Sample(Index(index), r)) //TODO: do we need a valid value here? 
      case (Some(d), None)    => Some(Sample(Index(index), d)) //no range, so make domain the range of an index function
      case (None, None) => ??? //TODO: nothing projected, could return Null but is it an error if we get this far?
    }
  }
}


object ProjectedFunction {
  
  def apply(function: Function, projection: Projection) = new ProjectedFunction(function, projection)
}