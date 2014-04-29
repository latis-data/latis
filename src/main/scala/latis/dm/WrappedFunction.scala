package latis.dm

import latis.data.SampleData
import latis.ops.Operation
import latis.util.DataUtils
import latis.util.PeekIterator2

//TODO: SampleMappedFunction?
class WrappedFunction(function: Function, val operation: Operation) 
  extends SampledFunction(function.getDomain, function.getRange) {
  
  /**
   * Override iterator to apply the Operation to each sample as it iterates (lazy).
   */
  override def iterator: Iterator[Sample] = new PeekIterator2(function.iterator, (s: Sample) => operation.applyToSample(s))
  
  /**
   * Get an Iterator of SampleData for this Function. This is really a step backwards
   * since it uses the wrapped Function's iterator which has already put the original Data into Samples.
   */
  //only needed for projection?
  override def getDataIterator: Iterator[SampleData] = iterator.map(DataUtils.sampleToData(_))

}

object WrappedFunction {
  def apply(function: Function, operation: Operation) = new WrappedFunction(function, operation)
}

  
  