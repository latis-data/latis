package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.NextIterator

/**
 * Wrapper for a Function that applies a "projection" to each sample.
 * The resulting Function will include only the Variables named in the Projection.
 */
class ProjectedFunction(function: Function, val projection: Projection) extends Function(function.domain, function.range) {
  /*
   * TODO: domain and range may change
   * but we pass them to super
   * simply override?
   * seems dangerous
   * pass obvious dummies to super?
   * domain and range should probably be methods
   */
  
  /*
   * TODO: 2013-09-17
   * seems that we should be able to do this as a Dataset wrapper
   * instead of making this a subclass of Function, with the domain/range issues...
   * But we'll be iterating into tree of variables and losing context?
   * Will we ever do this for internal Functions?
   * not any time soon, only used for iteration over outer function
   * 
   * Writer just recurses tree, treats dataset as any tuple
   * when it gets to function it calls iterate
   * no chance to wrap it
   */
  
  override def domain: Variable = _domain
  override def range: Variable = _range
  
  //delegate to projection to get new domain and range for the model
  //TODO: make sure this doesn't tickle the data
  val (_domain, _range) = projection.projectSample(Sample(function.domain, function.range)) match {
    case Some(sample: Sample) => (sample.domain, sample.range)
    case None => ??? //TODO: nothing projected
    case _ => ??? //TODO: something other than Sample returned
  }
  
  override def iterator = new NextIterator[Sample] {
    lazy val it = function.iterator  //original Function's iterator
    
    override def getNext: Sample = {
      if (it.hasNext) {
        val nxt = it.next()
        projection.projectSample(nxt) match {
          //TODO: keep index counter and pass that in so proj can replace a non-projected domain? yikes
          case Some(sample: Sample) => sample
          case _ => throw new Error("Projected sample should be a 2-Tuple.")
        }
      } else null //no more valid samples
    }
  }
}

object ProjectedFunction {
  
  def apply(function: Function, projection: Projection) = new ProjectedFunction(function, projection)
}