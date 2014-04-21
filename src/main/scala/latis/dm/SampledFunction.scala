package latis.dm

import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.EmptyData
import latis.data.Data
import latis.util.DataUtils

class SampledFunction(domain: Variable, range: Variable, _iterator: Iterator[Sample] = null,
    metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
    extends AbstractVariable(metadata, data) with Function {
  
  /*
   * 2013-08-07
   * TODO: ContinuousFunction
   * now that Data is separable, how can we support continuous function (e.g. exp model)
   * ContinuousFunction:
   *   apply(domainVal: Variable) => range val
   *   apply(domainSet: Seq[Variable] or Var with SeqData) => SampledFunction
   *   length = -1?
   *   iterator => error
   */
  
  /*
   * TODO: IterableOnce issues
   * Use Stream? or cache?
   * effectively becomes like a GranuleAdapter after the first pass
   * but requires caching even if we don't need it, oh well
   * responsibility of IterativeAdapter to cache as it iterates
   * property of adapter for when we really can't cache?
   * 
   * iterator.toStream here?
   * or add 'stream' in place of 'iterate'?
   * need hook to disable for really large datasets
   */
  
  //expose domain and range via defs only so we can override (e.g. ProjectedFunction)
  def getDomain: Variable = domain
  def getRange: Variable = range
  
  //private var _iterator: Iterator[Sample] = null
  
  def iterator: Iterator[Sample] = {
    if (_iterator != null) _iterator
    else if (getData.isEmpty) iterateFromKids
    else getDataIterator.map(DataUtils.dataToSample(_, Sample(domain, range)))
  }
  
  private def iterateFromKids: Iterator[Sample] = {
    //val dit = domain.getDataIterator.map(data => DataUtils.dataToVariable(data, domain))
    val dit = domain.getDomainDataIterator.map(data => DataUtils.dataToVariable(data, domain))
    val rit = range.getDataIterator.map(data => DataUtils.dataToVariable(data, range))
    (dit zip rit).map(pair => Sample(pair._1, pair._2))
  }

  
  //Support first and last filters
  //TODO: consider more optimal approaches
  //TODO: consider immutability, iterator position
  def getFirstSample: Sample = iterator.next //TODO: peek
  def getLastSample: Sample = {
    //iterator.drop(length-1).next  //dataIterator is giving DataUtils.dataToSample null Data!?
    var sample: Sample = null
    for (s <- iterator) sample = s
    sample 
  }
  
  def getSample(index: Int): Sample = {
    iterator.drop(index-1).next
  }

}
