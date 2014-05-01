package latis.reader.tsml

import latis.data.Data
import latis.data.IterableData
import latis.data.SampleData
import latis.data.SampledData
import latis.data.set.IndexSet
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Variable
import latis.reader.tsml.ml.Tsml
import latis.util.DataUtils
import latis.util.MappingIterator

import scala.collection.Map

/**
 * Base class for Adapters for data sources that have 'record' semantics.
 */
abstract class IterativeAdapter[R](tsml: Tsml) extends TsmlAdapter(tsml) {
  //R is the type of record
  
  /**
   * Override to provide an Iterator of records.
   */
  def getRecordIterator: Iterator[R] //TODO: return same one or make new one? hook to iterate more than once?
  
  /**
   * Override to parse a record and put it into a Map with variable name mapped to its Data.
   */
  def parseRecord(record: R): Option[Map[String,Data]] 
  
  
  /**
   * Construct the Data for a SampledFunction to support its iteration.
   * This is typically done lazily as the client iterates on the Function.
   */
  def makeSampledData(sampleTemplate: Sample): SampledData = {
    val domainTemplate = sampleTemplate.domain
    val rangeTemplate  = sampleTemplate.range
    
    //TODO: consider other ways to define domain set in tsml
    
    if (domainTemplate.isInstanceOf[Index]) {
      //Represent the domain as an IndexSet which will produce the appropriate values as it iterates.
      //TODO: consider nD domain with one or more Index dims
      val dset = IndexSet()
      //TODO: if Function length is defined
      //  getLength match {
      //  case n: Int if (n < 0) => IndexSet() //undefined length
      //  case n: Int => IndexSet(n)
      //}
      val rdata = makeIterableData(rangeTemplate, getRecordIterator)
      SampledData(dset, rdata)
    } else {
      //Make Data by wrapping an iterator with a function that lazily parses the data.
      val it: Iterator[SampleData] = {
        //function composition and partial application
        val f = parseRecord _ andThen makeSampleDataFromDataMap(sampleTemplate) _
        new MappingIterator(getRecordIterator, f)
      }
      SampledData(it, sampleTemplate) //will make duplicate/coupled iterators with unexplored caching implications
    }
  }
  
  /**
   * Create IterableData that lazily parses the data for the given Variable definition.
   */
  private def makeIterableData(template: Variable, recordIterator: Iterator[R]) = new IterableData {
    def recordSize: Int = template.getSize
    //function composition and partial application
    val f = parseRecord _ andThen makeDataFromDataMap(template) _
    def iterator = new MappingIterator(recordIterator, f)
  }
  
  /**
   * Create Data from a Data Map for the given Variable definition.
   */
  private def makeDataFromDataMap(template: Variable)(dataMap: Option[Map[String,Data]]): Option[Data] = dataMap match {
    case None => None
    case Some(m) => Some(DataUtils.makeDataFromDataMap(m, template))
    //TODO: should DataUtils.makeDataFromDataMap return Option?
  }
    
  /**
   * Create SampledData from a Data Map for the given Sample definition.
   */
  private def makeSampleDataFromDataMap(sampleTemplate: Sample)(dataMap: Option[Map[String,Data]]): Option[SampleData] = dataMap match {
    case None => None
    case Some(m) => Some(DataUtils.makeSampleDataFromDataMap(m, sampleTemplate))
  }
  
  
  /**
   * Override to make Function with IterableData.
   */
  override def makeFunction(f: Function): Option[Function] = {
    val template = Sample(f.getDomain, f.getRange)
    makeSample(template) match {
      case Some(sample) => {
        val data: SampledData = makeSampledData(sample)
        Some(Function(sample.domain, sample.range, f.getMetadata, data))
      }
      case None => None
    }
  }
  
}
