package latis.dm

import latis.data.SampleData
import latis.ops.Operation
import latis.util.DataUtils
import latis.util.iterator.MappingIterator
import latis.util.iterator.PeekIterator
import com.typesafe.scalalogging.LazyLogging
import latis.metadata.Metadata

//TODO: SampleMappedFunction?
class WrappedFunction(function: Function, val operation: Operation) 
  extends SampledFunction(function.getDomain, function.getRange) with LazyLogging {
  
  logger.debug("Making WrappedFunction for " + function +" " + operation)
  
  /**
   * Delegate to wrapped Function to get metadata.
   * TODO: The Operation should be responsible for updating the metadata
   *   such as filters that could specify a length.
   */
  override def getMetadata(): Metadata = function.getMetadata()
  
  /**
   * Override iterator to apply the Operation to each sample as it iterates (lazy).
   */
  override def iterator: PeekIterator[Sample] = {
    logger.debug("WrappedFunction iterating Samples: " + this)
    new MappingIterator(function.iterator, (s: Sample) => operation.applyToSample(s))
  }
  
  /**
   * Get an Iterator of SampleData for this Function. This is really a step backwards
   * since it uses the wrapped Function's iterator which has already put the original Data into Samples.
   */
  //only needed for projection?
  override def getDataIterator: Iterator[SampleData] = {
    logger.debug("WrappedFunction iterating Data: " + this)
    iterator.map(DataUtils.sampleToData(_))
  }

  //override def toString = super.toString + " + " + operation
}

object WrappedFunction {
  def apply(function: Function, operation: Operation) = new WrappedFunction(function, operation)
}

  
  