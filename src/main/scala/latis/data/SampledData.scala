package latis.data

import latis.data.seq.DataSeq
import latis.data.set.DomainSet
import latis.data.set.RealSampledSet
import latis.data.value.DoubleValue
import latis.dm.Sample
import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.slf4j.Logging
import latis.util.PeekIterator

class SampledData extends IterableData with Logging {
  
  //TODO: get length from domain set
  
  def recordSize: Int = domainSet.recordSize + rangeData.recordSize
  
  private var _domain: DomainSet = null
  private var _range: IterableData = null
  
  //TODO: if null, iterate and cache both?
  def domainSet: DomainSet = _domain
  def rangeData: IterableData = _range

  /*
   * TODO: cache to avoid iterable once problems
   * (should try to avoid iterating twice, debug log?)
   * make special subclass and let Function constructor (e.g. adapter) decide which to use
   * cache is only complete if the original iteration was not interrupted?
   * thus GranuleAdapter should force full iteration, or EagerlyCachedSampledData
   * consider Stream, ehcache...
   * 
   * manage iterator coupling with cache instead of using duplicate?
   * but only if we do cache, subclass thing?
   * 
   * +define length after caching
   */
  
  private val dataCache = ArrayBuffer[SampleData]()
  
  private def pairToSample = (ddata: Data, rdata: Data) => {
    val sd = SampleData(ddata, rdata)
    //TODO: enable cache    dataCache += sd
    sd
  }
  
  def iterator: Iterator[SampleData] = {
    if (dataCache.isEmpty) {
      logger.debug("Make SampleData Iterator from domain and range Data")
      val dit = _domain.iterator
      val rit = _range.iterator
      (dit zip rit).map(p => pairToSample(p._1, p._2))
    } else {
      logger.debug("Make SampleData Iterator from cache.")
      dataCache.iterator
    }
  }
  
}

object SampledData {
  
  def apply(domainData: IterableData, rangeData: IterableData): SampledData = SampledData(DomainSet(domainData), rangeData)
    
  def apply(domainSet: DomainSet, rangeData: IterableData): SampledData = {
    val sd = new SampledData
    sd._domain = domainSet
    sd._range = rangeData
    sd
  }
  
  //TODO: explore implications of duplicated Iterators (e.g. its caching)
  //TODO: should we manage our own internal iterator and caching?
  //TODO: require PekIterator then no need for template?
  def apply(sampleIterator: Iterator[SampleData], sampleTemplate: Sample): SampledData = {
    val (dit,rit) = sampleIterator.duplicate
    val dset = DomainSet(IterableData(dit.map(_.domainData), sampleTemplate.domain.getSize))
    val rdata = IterableData(rit.map(_.rangeData), sampleTemplate.range.getSize)
    SampledData(dset, rdata)
  }
  
  def fromValues(dvals: Seq[Double], vals: Seq[Double]*) ={
    //assert that all Seq are same length as the domain
    if (vals.exists(_.length != dvals.length)) throw new Error("Value sequences must be the same length.")
    
    val dset = RealSampledSet(dvals)
    //DataSeq(ds.map(DoubleValue(_)))
    val d1: IterableData = IterableData(vals.head.map(DoubleValue(_)))
    val f = (ds1: IterableData, ds2: Seq[Double]) => ds1 zip DataSeq(ds2.map(DoubleValue(_)))
    val rdata = vals.tail.foldLeft(d1)(f)
    SampledData(dset, rdata)
  }
}