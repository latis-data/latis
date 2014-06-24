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
import latis.util.MappingIterator
import latis.data.EmptyData
import com.typesafe.scalalogging.slf4j.Logging

class SampledFunction(domain: Variable, range: Variable, metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
    extends AbstractVariable(metadata, data) with Function with Logging {

  //expose domain and range via defs only so we can override
  //TODO: no longer the case? require operation to make new SampledFunction if it changes the type instead of a wrapped function
  def getDomain: Variable = domain
  def getRange: Variable = range
  
  /**
   * Return the number of samples represented by this SampledFunction.
   */
  def getLength: Int = {//TODO: long?
    //TODO: is length too overloaded? getSampleCount?
    //TODO: look at domain data
    //TODO: consider -n = unlimited but currently at n?
    //get from metadata, too dangerous to get from iterator: IterableOnce, nested Function...
    getMetadata.get("length") match {
      case Some(l) => l.toInt
      case None => throw new Error("Function length not defined.")
    }
  }
  
  /**
   * Internal Iterator so we can construct a SampledFunction from an Iterator of Samples.
   */
  private var _iterator: Iterator[Sample] = null
  
  /**
   * If this SampledFunction was constructed with an Iterator, return it.
   * Wrap it as a PeekIterator if it isn't one already.
   * Otherwise, make the Iterator from the Data Iterator.
   */
  def iterator: PeekIterator[Sample] = _iterator match {
    case null => {
      logger.debug("Make Iterator from DataIterator: " + this)
      new MappingIterator(getDataIterator, (d: Data) => Some(DataUtils.dataToSample(d, Sample(domain, range))))
    }
    case pit: PeekIterator[Sample] => {
      logger.debug("Return existing Iterator: " + this)
      pit
    }
    case _ => {
      logger.debug("Wrap existing Iterator: " + this)
      new MappingIterator(_iterator, (s: Sample) => Some(s))
    }
  }
  
  /**
   * If this SampledFunction was constructed with SampledData, iterate on it.
   * Otherwise, try to use the sample iterator and map back to the data.
   */
  def getDataIterator: Iterator[SampleData] = {
    val d = getData
    if (d.isEmpty) {
      logger.debug("Make Data Iterator from existing Sample Iterator: " + this)
      new MappingIterator(_iterator, (s: Sample) => Some(DataUtils.sampleToData(s)))
    } else {
      logger.debug("Make Data Iterator from SampledData: " + this)
      d.asInstanceOf[SampledData].iterator
    }
  }
}

object SampledFunction {
  
  def apply(domain: Variable, range: Variable, samples: Iterator[Sample], metadata: Metadata = EmptyMetadata) = {
    val sf = new SampledFunction(domain, range, metadata=metadata)
    sf._iterator = samples
    sf
  }
}