package latis.reader.tsml

import latis.data.Data
import latis.data.IterableData
import latis.dm.Function
import latis.dm.Sample
import latis.reader.tsml.ml.Tsml
import latis.util.DataUtils
import latis.util.PeekIterator2
import scala.collection.Map
import latis.util.IndexedIterator
import latis.data.IndexData

/**
 * Base class for Adapters for data sources that have 'record' semantics.
 * By default, data will be cached so we don't have an IterableOnce problem.
 */
abstract class IterativeAdapter[R](tsml: Tsml) extends TsmlAdapter(tsml) {
  //R is the type of record
  
  def getRecordIterator: Iterator[R] //TODO: return same one or make new one? hook to iterate more than once?
  def parseRecord(record: R): Option[Map[String,Data]] 
  
  def parseRecordWithIndex(record: R, index: Int): Option[Map[String,Data]] = {
    parseRecord(record) match {
      case Some(dataMap) => {
        //replace Index data with current index
        if (dataMap.contains("index")) Some(dataMap + ("index" -> IndexData(index)))
        else Some(dataMap)
      }
      case None => None
    }
  }
  
  private lazy val parsedRecordIterator = new IndexedIterator(getRecordIterator, (record: R, index: Int) => parseRecordWithIndex(record, index))
  //def getCurrentIndex = parsedRecordIterator.getIndex
  
  def makeDataIterator(sampleTemplate: Sample): Iterator[Data] = {
    if (cacheIsEmpty) {
      new PeekIterator2(parsedRecordIterator, (vals: Map[String,Data]) =>  makeDataFromValueMap(vals, sampleTemplate))
    } else {
      getCachedData("sample") match {
        case Some(data) => data.iterator
        case None => throw new Error("No data in the cache for: sample")
      }
    }
  }
    
  private def makeDataFromValueMap(dataMap: Map[String,Data], sampleTemplate: Sample): Option[Data] = {
    val data = DataUtils.makeDataFromDataMap(dataMap, sampleTemplate, parsedRecordIterator.getIndex)
    
    //cache based upon caching strategy //TODO: async?
    getProperty("cache") match {
      case Some("none") =>
      case _ => appendToCache("sample", data)
    }
    
    Some(data)
  }
  
  /**
   * Override to make Function with IterableData.
   */
  override def makeFunction(f: Function): Option[Function] = {
    //Note, if domain or range is None (e.g. not projected), make index function
    val template = Sample(f.getDomain, f.getRange)
    makeSample(template) match {
      case Some(sample) => {
        val data: Data = makeIterableData(sample)
        Some(Function(sample.domain, sample.range, f.getMetadata, data))
      }
      case None => None
    }
  }
  
  def makeIterableData(sampleTemplate: Sample): Data = new IterableData {
    def recordSize = sampleTemplate.getSize
    def iterator = makeDataIterator(sampleTemplate)
  }
}
    /*
     * ascii adapters' parseRecord returns String instead of Data
     * ++can we simply use StringValue? size issue when converting to bytes? apply when building from template
     * do we need AsciiParser trait if we do away with GranuleAsciiAdapter?
     * get rid of AsciiAdapterHelper, too?
     * they are both record centric, no need to have granule adapter to share with 
     *   since the granule adapter can be an iterative adapter with eager caching
     * row vs column major storage concerns?
     *   granule cache was col-oriented, not helpful?
     * +some data require sorting
     *   if source isn't sorted then we can have only a index Function, use Factorization to define domain and sort  
     *    
     * ***want to be able to separate out domain Data as Sets
     *   other vars' data could use set or function mapping
     * column-oriented cache
     *   nested Functions could simply use recordSize
     * but Iterative adapter can't have domain sets
     *   could via aggregation
     *   ++domain defined as values in tsml could be a set
     *     is this done via aggregation?
     * just stick with column-oriented for now
     *   optimize for record oriented later? 
     *   akin to storing data in Tuples, not yet supported
     * since the adapter owns the cache, we could make it as adapter specific as we'd like
     *   but would like to reuse some utils
     *   seems reasonable for IterativeAdapter to store data by sample
     *   
     * ++do we need to consider multiple Functions? more than one "sample" variable
     * let's assume not, for now
     * Sample.getName = sample
     * 
     * ++consider cache as DatasetAccessor?
     *   Dataset effectively has cache instead of Adapter
     *   consider making data in code, no adapter
     *   already tried this in earlier iterations, before we had Data as part of Variable?
     *   if every dataset needs to have an Accessor, what does a derived dataset have?
     *   currently Dataset doesn't know its accessor/adapter
     *     except provenance metadata
     * consider need for cache vs Var having data
     *   latter much easier for DSL use
     *   maybe it is more of an adapter issue: holding data so we can iterate on it again
     */
    
//    getRecordIterator.map(record => {
//      parseRecord(record) //Map[String,Data]
//      //TODO: stitch Data together based on sample template
//      makeDataFromRecord(sampleTemplate, parsedRecord, currentIndex)
//      /*
//       * index of recordIt or dataIt?
//       * try recordIt, assume adapter can apply PIs before exposing recordIt?
//       * but currently, parseRecord is responsible for rejecting records, return empty Map
//       * make another PeekIt that wraps recordIt and applies parse, skipping bad samples
//       *   parsedRecordIterator
//       *   use Option instead of emptyMap
//       * 
//       * need another dataset building pass?
//       *   1) orig ds, model + metadata only
//       *   2) apply PIs then cache
//       *   3) apply user ops
//       */
//    })
    


/**
 * This Adapter is designed for arbitrarily large Datasets that can be
 * processed one sample at a time. The Data will be managed in the Function
 * via the Data's iterator which can be fed by this Adapter.
 */
abstract class IterativeAdapterORIG(tsml: Tsml) extends TsmlAdapter(tsml) {
  //TODO: Stream, lazy list?
  //TODO: consider Iterative and Granule as traits?
  //TODO: consider use cases beyond single top level variable = Function
  
  //will be invoked when client tries to iterate on IterableData via iterating on WrappedFunction
  //lazy val dataIterator = makeDataIterator
  lazy val dataIterator = ??? //new PeekIterator2(getRecordIterator, (record: Any) => parseData(record))
//TODO: abstract out parts to be overridden
//  def getRecordIterator: Iterator[Any]
 // def parseData(record: Any): Option[Data]
  
  /*
   * TODO: Iterative vs Granule as trait?
   * both based on 
   *   recordIterator
   *   parseRecord => vname -> data
   * Iterative does PeekIterator2 as above
   * Granule has readData that adds parsed record to cache
   * parameterize adapter with record type?
   * keep in mind that trait can't have state
   *   can't hold on to dataIterator to get index...
   *   use some sort of inner class within a function?
   *   return an object with the state?
   * 
   * ***instead of putting all data in scalars in granule adapter, just use cache
   * use same iterator approach but get data from cache instead of source
   * iterable adapter could cache ech sample then act like a granule adapter
   * do we even need granule adapter?
   *   probably for not iterable sources, e.g. netcdf
   *   one readAll sucks data into cache
   * IterableAdapter: record based
   *   recordIterator: Iterator[R]
   *   parameterize with record type: IterableAdapter[String]
   *   can that be used as trait?
   *   maybe not needed since many granule adapters could be impld as Iterative with caching enabled
   * is it worth worrying about column vs row major
   *   cache as column major
   *   but iterative is row major
   *   col major cache means we can do some things better from cache
   *   Peek2 used to map sample template to sample with data
   *   can we do this in terms of parseRecord: Map[Name,Data]?
   *   consider utils: sample2data, data2sample
   *   dataIterator: 
   *     based on Sample template (or any Var?)
   *     if cache is empty: recordIterator.map(parseRecord(_))
   *       if cache is not none, cache by sample: "sample" -> Data for the entire sample
   *     else pull data from cache, effectively need new Adapter to serve data from cache
   *     could this be a mixin?
   *   consider caching by sample? row major
   *     key/name = "sample"?
   *     but parseRecord returns map of scalar name to Data
   *   does cache know record size?
   *     cache doesn't need to be iterable, up to adapter to manage how to get data out
   *   
   *     
   *   WrappedFunction uses PeekIt to map sample to sample via operation (SampleHomomorphism)
   *   IterativeAdapter uses PeekIt to map record to Data, doesn't care about sample structure (though parsing will)
   *   
   *   +++
   *   
   * Hibernate has "load" vs "iterate"
   * 
   * cache: map name to Data = byte buffer
   * use recordSize to do index math?
   * use ehcache so not limited by memory
   * 'cache' property in adapter tsml: default or none, lazy vs eager?
   * ++property for iterative with or without lazy cache vs granule with eager cache
   * 
   * ehcache
   *   singleton cm = CacheManager.create
   *   add multiple Cache-s (one for each Dataset)
   *   manager.addCache(new Cache(config))
   *   manager.getCache("my_dataset")
   *   cache.put(new Element(key, value))   (key = var name)
   *   elm = cache.get(key)
   *   elm.getValue (Serializable, byte array)
   * we would want to append to value as we iterate
   * consider key/value store for disk cache?
   */
  
  
  /**
   * Implementations of IterativeAdapter need to override this so we can construct Data
   * that can iterate over each sample.
   */
  def makeDataIterator = dataIterator
//TODO: abstract out part to be overridden

  //def makeIterableData(sampleTemplate: Sample): Data
  
  private var sampleTemplate: Sample = null
  def getSampleTemplate = sampleTemplate
  lazy val getSampleSize = sampleTemplate.getSize
  def getCurrentIndex: Int = ??? //dataIterator.getIndex
  
  /**
   * Override to make Function with IterableData.
   */
  override def makeFunction(f: Function): Option[Function] = {
    //if domain or range is None (e.g. not projected), make index function
    val template = Sample(f.getDomain, f.getRange)
    makeSample(template) match {
      case Some(sample) => {
        //sampleTemplate = sample
        val data: Data = makeIterableData(sample)
        Some(Function(sample.domain, sample.range, f.getMetadata, data))
      }
      case None => None
    }
  }
  
  def makeIterableData(sampleTemplate: Sample): Data = new IterableData {
    lazy val recordSize = sampleTemplate.getSize
    lazy val iterator = dataIterator
  }
}
