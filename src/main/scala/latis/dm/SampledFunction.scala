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

class SampledFunction(domain: Variable, range: Variable, metadata: Metadata = EmptyMetadata, data: SampledData = EmptyData) 
    extends AbstractVariable(metadata, data) with Function with Logging {

  //expose domain and range via defs only so we can override
  //TODO: no longer the case? require operation to make new SampledFunction if it changes the type instead of a wrapped function
  def getDomain: Variable = domain
  def getRange: Variable = range
  def getSample: Sample = Sample(domain, range)
  
  
  //evaluate, use resample Operation for SampledFunctions
  def apply(arg: Variable): Option[Variable] = {
    
    
    ???
  }
  
  
  /**
   * Return the number of samples represented by this SampledFunction.
   */
  def getLength: Int = { //TODO: long?
    //TODO: consider -n = unlimited but currently at n?
    
    //try metadata
    getMetadata.get("length") match {
      case Some(l) => l.toInt
      case None => {
        //try looking at data
        if (data.notEmpty) {
          data.domainSet.length //may be undefined/unlimited = -1
        } else {
          //get length from iterator
          //TODO: IterableOnce problem, cache?
          logger.debug("Using iterator to get length of Function " + this)
          iterator.length
        }
      }
    }
  }
  
  /**
   * Internal Iterator so we can construct a SampledFunction from an Iterator of Samples.
   */
  private var _iterator: Iterator[Sample] = null
  
  //counter for testing iterable once problems
  private var itcounter = 0
  
  /**
   * If this SampledFunction was constructed with an Iterator, return it.
   * Wrap it as a PeekIterator if it isn't one already.
   * Otherwise, make the Iterator from the Data Iterator.
   */
  def iterator: PeekIterator[Sample] = {
    //test if we have tried to iterate more than once on this function
    itcounter += 1
//    if (itcounter > 1) throw new Error("Iterating more than once on " + this)
    
    _iterator match {
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
      PeekIterator(_iterator)
    }
    }
  }
  
  /**
   * If this SampledFunction was constructed with SampledData, iterate on it.
   * Otherwise, try to use the sample iterator and map back to the data.
   */
  def getDataIterator: Iterator[SampleData] = {
    val d = getData
    if (d.isEmpty) {
      //Data is presumably already dispersed throughout model, in Scalars
      //TODO: try to avoid this, not efficient
      logger.debug("Make Data Iterator from existing Sample Iterator: " + this)
      new MappingIterator(_iterator, (s: Sample) => Some(DataUtils.sampleToData(s)))
    } else {
      logger.debug("Make Data Iterator from SampledData: " + this)
      d.asInstanceOf[SampledData].iterator
    }
  }
}

object SampledFunction {
  //TODO: redundant with Function constructors?
  def apply(domain: Variable, range: Variable, samples: Iterator[Sample], metadata: Metadata = EmptyMetadata) = {
    val sf = new SampledFunction(domain, range, metadata=metadata)
    if (samples == null) throw new Error("Can't construct a SampledFunction with a null Sample Iterator.")
    sf._iterator = samples
    //TODO: should we make SampledData instead? resolve iterable once problem by caching in SampledData
    sf
  }
}