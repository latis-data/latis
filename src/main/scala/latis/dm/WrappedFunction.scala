package latis.dm

import latis.ops.IndexedSampleMappingOperation
import latis.util.IndexedIterator

//TODO: MappedFunction?
class WrappedFunction(function: Function, val operation: IndexedSampleMappingOperation) 
  extends SampledFunction(function.getDomain, function.getRange) {
  //TODO: may need to override getDomain, getRange if the types change
  
  /**
   * Override iterator to apply the Operation to each sample as it iterates (lazy).
   */
  override def iterator: Iterator[Sample] = new IndexedIterator(function.iterator, (s: Sample, index: Int) => operation(s, index))
}

object WrappedFunction {
  def apply(function: Function, operation: IndexedSampleMappingOperation) = new WrappedFunction(function, operation)
}