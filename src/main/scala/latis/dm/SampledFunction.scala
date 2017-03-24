package latis.dm

import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.EmptyData
import latis.data.Data
import latis.util.DataUtils
import latis.data.IterableData
import latis.data.SampledData
import latis.data.set.IndexSet
import latis.data.SampleData
import latis.util.iterator.PeekIterator
import latis.util.iterator.MappingIterator
import latis.data.EmptyData
import com.typesafe.scalalogging.LazyLogging

/**
 * A SampledFunction represents a discrete Function whose values are defined
 * by Sample objects. Each Sample object represents a single mapping of a
 * domain value to a range value.
 */
class SampledFunction(domain: Variable, range: Variable, metadata: Metadata = EmptyMetadata, data: SampledData = EmptyData) 
    extends AbstractVariable(metadata, data) with Function with LazyLogging {

  //expose domain and range via defs only so we can override
  //TODO: no longer the case? require operation to make new SampledFunction if it changes the type instead of a wrapped function
  def getDomain: Variable = domain
  def getRange: Variable = range
  def getSample: Sample = Sample(domain, range)
  
  
  //evaluate
  //TODO: use interpolation and extrapolation strategies
  def apply(arg: Variable): Option[Variable] = {
    val x = iterator.find(s => (s.domain, arg) match {
      case (a: Scalar, b: Scalar) => a.compare(b) == 0
      case _ => throw new IllegalArgumentException("Cannot evaluate a Function with a non-Scalar domain.")
    })
    
    x match {
      case Some(s: Sample) => Some(s.range)
      case None => None
    }
  }
  
  /**
   * Compose two functions such that the resulting function has the domain of g
   * and the range of this.
   */
  def compose(g: Function): Function = {
    _iterable = iterator.toSeq //need to be able to reuse the data
    val it = new MappingIterator(g.iterator, (s: Sample) => this(s.range) match {
      case Some(v) => Some(Sample(s.domain, v))
      case None => None
    }) 
    
    Function(g.getDomain, range, it, g.getMetadata + ("name" -> s"${this.getName} of ${g.getName}"))
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
        if(_iterable != null) _iterable.size 
        //try looking at data
        else if (data.notEmpty) {
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
   * Internal Iterable so we can construct a SampledFunction from an Iterator of Samples.
   */
  private var _iterable: Iterable[Sample] = null
  
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
    
    _iterable match {
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
      PeekIterator(_iterable.iterator)
    }
    }
  }
  
  /**
   * Returns true if iterator.isEmpty returns true,
   * else false
   * 
   * The advantage of calling this method over
   * iterator.isEmpty (aside from being shorter) is
   * that since iterator returns a new PeekIterator wrapping
   * our private _iterator, calling PeekIterator
   * will consume an element of _iterator. This
   * method will not consume any elements from
   * _iterator (it calls _iterator.hasNext directly)
   * 
   * This problem manifests itself if you run code like this:
   * val isEmpty = myFunction.iterator.isEmpty
   * This causes a sample to be lost from myFunction._iterator
   * because the returned PeekIterator must read from
   * _iterator to verify that _iterator is not empty.
   * The value still exists inside the returned PeekIterator,
   * but since this code doesn't save a reference to that
   * object, both the PeekIterator and its cached value are
   * lost.
   */
  def isEmpty: Boolean = if (_iterable != null) {
    _iterable.isEmpty
  }
  else {
    getData.isEmpty
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
      if (_iterable == null || _iterable.isEmpty) Iterator.empty
      else new MappingIterator(_iterable.iterator, (s: Sample) => Some(DataUtils.sampleToData(s)))
    } else {
      logger.debug("Make Data Iterator from SampledData: " + this)
      domain match {
        case i: Index => SampledData(IndexSet(), d.asInstanceOf[SampledData].rangeData).iterator
        case _ => d.asInstanceOf[SampledData].iterator
      }
    }
  }
}

object SampledFunction {
  //TODO: redundant with Function constructors?
  def apply(domain: Variable, range: Variable, samples: Iterator[Sample], metadata: Metadata = EmptyMetadata) = {
    if (samples == null) throw new Error("Can't construct a SampledFunction with a null Sample Iterator.")
    val sit = domain match {
      case i: Index => IndexSet().iterator.zip(samples).map(p => Sample(Index(i.getMetadata, p._1), p._2.range))
      case _ => samples
    }
    val sf = new SampledFunction(domain, range, metadata=metadata){
      override def iterator = PeekIterator(sit)
      //TODO: this break isEmpty
    }
    sf
  }
  
  def apply(samples: Iterable[Sample], metadata: Metadata) = {
    if (samples == null) throw new Error("Can't construct a SampledFunction with a null Sample Iterable.")
    val template = samples.head
    val sit = template.domain match {
      case i: Index => Iterable.range(0,samples.size).zip(samples).map(p => Sample(Index(i.getMetadata, p._1), p._2.range))
      case _ => samples
    }
    val sf = new SampledFunction(template.domain, template.range, metadata=metadata)
    sf._iterable = sit
    sf
  }
}
