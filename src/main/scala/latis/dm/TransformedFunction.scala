package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.PeekIterator
import latis.ops.xform.Transformation
import latis.util.PeekIterator2

@Deprecated
class TransformedFunction(function: Function, val xform: Transformation) 
  extends SampledFunction(function.getDomain, function.getRange) {
  //TODO: override domain, range, metadata, data
  
  //override def iterator = new PeekIterator2(function.iterator, (s: Sample) => xform.transformSample(s))
  
//  override def iterator = new PeekIterator[Sample] {
//    lazy val it = function.iterator  //original Function's iterator
//    
//    override def getNext: Sample = {
//      if (it.hasNext) {
//        xform.transformSample(it.next()) match {
//          case null => getNext //keep trying until we get a valid sample
//          case s: Sample => s
//          //TODO: could have been transformed into something other than Sample?
//        }
//      } else null //no more valid samples
//    }
//  }
}

object TransformedFunction {
  def apply(function: Function, xform: Transformation) = new TransformedFunction(function, xform)
}