package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.NextIterator
import latis.ops.xform.Transformation


class TransformedFunction(function: Function, val xform: Transformation) extends Variable with Function {
  //TODO: override domain, range, metadata, data ?
  
  /*
   * 2013-08-07
   * projection and selection are now unique
   *   selection: filters samples, same type, low level metadata won't change, only provenenance? maybe function (e.g. length)
   *   projection: "filters" variables, changes type, scalar metadata won't change
   * two types of other ops:
   *   modify values: same types, model won't change but scalar metadata might
   *     e.g. units
   *     other use cases?
   *       scale, offset - still essentially units
   *       replace missing... - still very similar
   *       replace wavelength with frequency? too much? effectively changes type? even if still Real
   *   transform model: could be entirely different
   * need more than just "Transformation" to capture above 2 cases
   *   can we get by with one? too much special logic?
   * 
   * Other ops just to munge/enhance metadata?
   * 
   * Derived Field as Operation
   * tsml variable type vs PI
   *   applicable beyond tsml
   *   equation in projection clause? or "function"?
   * but derived field makes (i.e. returns) a new Variable
   *   operation takes input ds, returns new ds with the result
   *   adding it to a Dataset is another step, aggregation
   *   
   * CompositeOperation
   *   
   */
  
  /*
   * TODO: 2013-07-15
   * feels wrong to pass orig domain, range to super
   * ++ see ProjectedFunction, override domain, range defs
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
   *   is this internal enough that directly wrapping a Function is OK
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