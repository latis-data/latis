package latis.dm

import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.EmptyData
import latis.data.Data
import latis.util.DataUtils
import latis.data.IterableData
import latis.data.SampledData
import latis.data.SampleData
import latis.util.PeekIterator
import latis.util.PeekIterator2

class SampledFunction(domain: Variable, range: Variable, metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
    extends AbstractVariable(metadata, data) with Function {

  
  //expose domain and range via defs only so we can override (e.g. ProjectedFunction)
  //TODO: no longer the case? require operation to make new SampledFunction if it changes the type instead of a wrapped function
  def getDomain: Variable = domain
  def getRange: Variable = range
  
  //private var _iterator: Iterator[Sample] = null
  //TODO: do we have a use case to construct with a set of Samples as opposed to data values?
  
  def iterator: PeekIterator[Sample] = new PeekIterator2(getDataIterator, (d: Data) => Some(DataUtils.dataToSample(d, Sample(domain, range))))
  
//  {
////    if (_iterator != null) _iterator //TODO: do we use this? WrappedFunction may want to intervene at DataIterator (new SampledData)
////    else if (getData.isEmpty) ??? //iterateFromKids
////    else 
//    getDataIterator.map(DataUtils.dataToSample(_, Sample(domain, range)))
//    //TODO: use PeekIterator2(getDataIterator, (s: Sample) => DataUtils.dataToSample(_, Sample(domain, range))) ?
//    //  then users could peek
//  }
  
  //TODO: require SampledFunction to have SampledData?
  def getDataIterator: Iterator[SampleData] = getData.asInstanceOf[SampledData].iterator

}
