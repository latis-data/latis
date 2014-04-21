package latis.reader.tsml

import latis.dm._
import latis.data.Data
import latis.reader.tsml.ml.FunctionMl
import latis.reader.tsml.ml.Tsml
import latis.util.PeekIterator2
import latis.data.IterableData

trait IterativeAdapter2[R] { this: TsmlAdapter =>
  //R is the type of record
  
  def getRecordIterator: Iterator[R]
  def parseRecord(record: R): Map[String,Data]
  
  def getDataIterator: Iterator[Data] = {
    //if (cacheIsEmpty)
    getRecordIterator.map(record => {
      parseRecord(record) //Map[String,Data]
      //TODO: stitch Data together based on sample template
    })
    //else get from cache
    ???
  }
    
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
    
    lazy val iterator = ???//dataIterator
  }
}

/**
 * This Adapter is designed for arbitrarily large Datasets that can be
 * processed one sample at a time. The Data will be managed in the Function
 * via the Data's iterator which can be fed by this Adapter.
 */
abstract class IterativeAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
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
