package latis.dm

import latis.ops.IndexedSampleMappingOperation
import latis.util.IndexedIterator

/*
 * TODO: instead of a subclass for each class of operation, handle diffs here?
 */
//TODO: SampleMappedFunction?
class WrappedFunction(function: Function, val operation: IndexedSampleMappingOperation) 
  extends SampledFunction(function.getDomain, function.getRange) {
  //TODO: may need to override getDomain, getRange if the types change
  
  //if (operation.isInstanceOf[AlgebraicOperation]) 
  
  /**
   * Override iterator to apply the Operation to each sample as it iterates (lazy).
   */
  override def iterator: Iterator[Sample] = new IndexedIterator(function.iterator, (s: Sample, index: Int) => operation(s, index))
  /*
   * TODO: is it worth managing the index like this?
   * should we manage here in WrappedFunction?
   * but the operation knows what to do with it
   * see notes in IndexedSampleMappingOperation
   */
  
}

object WrappedFunction {
  def apply(function: Function, operation: IndexedSampleMappingOperation) = new WrappedFunction(function, operation)
}