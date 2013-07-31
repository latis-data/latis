package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.NextIterator
import latis.ops.xform.Transformation


class TransformedFunction(function: Function, val xform: Transformation) extends Function(function.domain, function.range) {
  //TODO: override domain, range, metadata, data ?
  /*
   * TODO: 2113-07-15
   * feels wrong to pass orig domain, range to super
   * this will likely change the model
   * should we drop all constructor args in favor of methods?
   *   internal state inserted by factory constructors
   *   seems more functional
   *   but current way is more like case classes
   *   
   * Consider better use of Traits
   * e.g. Function with methods for domain, range...?
   * with Data with Metadata?
   */
  
  /*
   * TODO: 2013-07-11
   * Should we just do a WrappedDataset?
   * Operation only operates on Dataset
   * seem to be doing things outside of that context by wrapping Function
   * how to add to provenance...?
   * or do we even need that?
   *   just make a new Dataset with the operator?
   *   but need to capture orig ds md
   * so how does this iterator get in place?
   *   at a high level, it is the dataset being operated on
   *   is this internal enough that directly wrapping a FUnction is OK
   *   assuming the code doing that will make sure the Dataset comes back together?
   *   the op needs to be in place when we start iterating on the Function
   *   can't look to parent Dataset to see it there is an operation for it
   *   
   * TODO: see withFilter, FilterMonadic
   * 
   * if we are just iterating on Vars, we could merge Filtered and TransformedFunction?
   * apply(var) and pattern match for Dataset?
   * but we want a static return type of Dataset
   * diff in use of Option
   */
  
  override def iterator = new NextIterator[Sample] {
    lazy val it = function.iterator  //original Function's iterator
    
    override def getNext: Sample = {
      if (it.hasNext) {
        xform.transformSample(it.next()) match {
          case null => getNext //keep trying until we get a valid sample
          case s: Sample => s
        }
      } else null //no more valid samples
    }
  }
}

object TransformedFunction {
  
  def apply(function: Function, xform: Transformation) = {
    new TransformedFunction(function, xform)
  }
  
}