package latis.ops

import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Index
import latis.dm.Variable

class IndexedSampleMappingOperation extends SampleMappingOperation {
  
  /*
   * TODO: instead of having IndexedIterator feed us the index
   * could we manage it here?
   * only increment if not None?
   * only filters and projections need this
   * not worth factoring out to this extent?
   * 
   * what about nested Functions
   * can they be wrapped?
   * they don't need to be iterable like outer functions
   * but same logic applies
   * can't manage index here, then
   * 
   * consider also multi-dim domain, sep index for each dim?
   * 
   * seems like WrappedFunction should own indexes
   * has access to iterator
   * but how does Operation get it since it knows how to apply it
   * 
   * How does this relate to DomainSets (as Data)?
   * could index be managed as IndexSet?
   * Function needs to own DomainSet 
   *   as part of Data?
   *   while iterating, building next sample, need to get values from domain set
   *   range as IterableData, map to each with same index
   *   should Function always keep domain and range data separate?
   *   or special kind of SampleData?
   *   still iterable
   *   IterableData for Function could be wrapped domain set + range data iterator
   *   IndexedIterator index used to get domain value
   * SampledFunction getDataIterator.map(DataUtils.dataToSample(_, Sample(domain, range))
   * SampledData?
   * 
   * could managing domainSet Data separate make sorting better?
   * 
   */
  //set when applied to sample and used for setting value in Index Scalars
  private var _index = -1
  def getIndex = _index
  
  def apply(sample: Sample, index: Int): Option[Sample] = {
    _index = index
    applyToSample(sample) match {
      case Some(s: Sample) => Some(s)
      case _ => None 
    }
  }
    
  /**
   * Override to update Index values.
   */
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    super.applyToScalar(scalar) match {
      case Some(_: Index) => Some(Index(_index))
      case Some(s: Scalar) => Some(s)
      case _ => None 
    }
  }
}