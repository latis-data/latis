package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.PeekIterator

/**
 * Wrapper for a Function that applies a "projection" to each sample.
 * The resulting Function will include only the Variables named in the Projection.
 * 
 */
class ProjectedFunction(function: Function, val projection: Projection) 
  extends WrappedFunction(function, projection) {
  //TODO: return data in projected order
  
  override def getDomain: Variable = _domain
  override def getRange: Variable = _range
  
  //Get the projected domain and range 'templates' (don't necessarily have data)
  lazy val (_domain, _range) = projection.applyToSample(Sample(function.getDomain, function.getRange)) match {
    case Some(sample) => (sample.domain, sample.range)
    case None => ??? //TODO: nothing projected, bug?
  }
  
}


object ProjectedFunction {
  
  def apply(function: Function, projection: Projection) = new ProjectedFunction(function, projection)
}