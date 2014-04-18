package latis.dm

import latis.metadata._
import latis.ops._
import latis.util.PeekIterator
import latis.ops.filter.Filter
import latis.util.PeekIterator2


class WrappedFunction(function: Function, val operation: SampleHomomorphism) 
  extends SampledFunction(function.getDomain, function.getRange) {
  //TODO: may need to override getDomain, getRange if the types change
  
  /**
   * Override iterator to apply the Operation to each sample as it iterates (lazy).
   */
  override def iterator = new PeekIterator2(function.iterator, (s: Sample) => operation(s))
}

object WrappedFunction {
  def apply(function: Function, operation: SampleHomomorphism) = new WrappedFunction(function, operation)
}