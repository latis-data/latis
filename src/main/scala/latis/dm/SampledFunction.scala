package latis.dm

import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.EmptyData
import latis.data.Data
import latis.util.Util

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
  
  //expose domain and range via defs only so we can override (e.g. ProjectedFunction)
  def getDomain: Variable = domain
  def getRange: Variable = range
  
  //private var _iterator: Iterator[Sample] = null
  
  def iterator: Iterator[Sample] = {
    if (_iterator != null) _iterator
    else if (getData.isEmpty) iterateFromKids
    else getDataIterator.map(Util.dataToSample(_, Sample(domain, range)))
  }
  
  private def iterateFromKids: Iterator[Sample] = {
    //val dit = domain.getDataIterator.map(data => Util.dataToVariable(data, domain))
    val dit = domain.getDomainDataIterator.map(data => Util.dataToVariable(data, domain))
    val rit = range.getDataIterator.map(data => Util.dataToVariable(data, range))
    (dit zip rit).map(pair => Sample(pair._1, pair._2))
  }

  
  //Support first and last filters
  //TODO: consider more optimal approaches
  //TODO: consider immutability, iterator position
  def getFirstSample: Sample = iterator.next //TODO: peek
  def getLastSample: Sample = {
    //iterator.drop(length-1).next  //dataIterator is giving Util.dataToSample null Data!?
    
    //TODO: only gets 190 of 570!?
    //trying to get new iterator with each ref? skipping by 3s
//    while(iterator.hasNext) {
//      sample = iterator.next
//      c = c+1
//      println(c +": "+ sample.domain.data)
//    }
    
    var sample: Sample = null
    for (s <- iterator) sample = s
    sample 
  }
  //TODO: return Function with single sample to preserve namespace...?

}
