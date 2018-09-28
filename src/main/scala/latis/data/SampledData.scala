package latis.data

import scala.collection.JavaConversions.mapAsScalaMap

import com.typesafe.scalalogging.LazyLogging

import latis.data.seq.DataSeq
import latis.data.set.DomainSet
import latis.data.set.RealSampledSet
import latis.data.value.DoubleValue
import latis.dm.Sample
import net.sf.ehcache.Cache
import net.sf.ehcache.CacheManager
import net.sf.ehcache.Element

class SampledData extends IterableData with LazyLogging {
  
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
  
  private var cacheEnabled = false
  
//  private val dataCache = ArrayBuffer[SampleData]()
  private lazy val dataCache: Cache = { //cache using Ehcache
    val manager = CacheManager.getInstance
    manager.addCacheIfAbsent("cache")
    manager.getCache("cache")
  }
  
  var index = 0 //use indexes so that samples can be accessed from cache in order
  
  private def pairToSample = (ddata: Data, rdata: Data) => {
    val sd = SampleData(ddata, rdata)
//    if(cacheEnabled) dataCache.put(new Element(index, sd)) 
//    index += 1
    sd
  }
  
  def iterator: Iterator[SampleData] = {
    if (!cacheEnabled || index == 0) { //first iteration, nothing in cache
      logger.debug("Make SampleData Iterator from domain and range Data")
      val dit = _domain.iterator
      val rit = _range.iterator
      (dit zip rit).map(p => pairToSample(p._1, p._2))
    } else { //access data from cache
      logger.debug("Make SampleData Iterator from cache.")
      val elems = dataCache.getAll(dataCache.getKeys) //unordered
      (0 until dataCache.getKeys.size).map(i => elems(i.asInstanceOf[java.lang.Integer]).getObjectValue.asInstanceOf[SampleData]).iterator
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
  
  def fromValues(dvals: Seq[Double], vals: Seq[Double]*): SampledData = {
    //assert that all Seq are same length as the domain
    if (vals.exists(_.length != dvals.length)) throw new RuntimeException("Value sequences must be the same length.")
    
    val dset = RealSampledSet(dvals)
    //DataSeq(ds.map(DoubleValue(_)))
    val d1: IterableData = IterableData(vals.head.map(DoubleValue(_)))
    val f = (ds1: IterableData, ds2: Seq[Double]) => ds1 zip DataSeq(ds2.map(DoubleValue(_)))
    val rdata = vals.tail.foldLeft(d1)(f)
    SampledData(dset, rdata)
  }
}