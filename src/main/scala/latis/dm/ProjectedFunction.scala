package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.NextIterator

/**
 * Wrapper for a Function that applies a "projection" to each sample.
 * The resulting Function will include only the Variables named in the Projection.
 */
class ProjectedFunction(function: Function, val projection: Projection) 
  extends SampledFunction(null, null) {
//pass null domain and range since we override them here
  
  //TODO: return data in projected order
  
  override def getDomain: Variable = _domain
  override def getRange: Variable = _range
  
  //delegate to projection to get new domain and range for the model
  //TODO: make sure this doesn't tickle the data
  val (_domain, _range) = projection.projectSample(Sample(function.getDomain, function.getRange)) match {
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