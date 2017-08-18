package latis.reader.adapter

import latis.data._
import latis.dm._
import scala.collection._

/**
 * Base class for Adapters for data sources that have 'record' semantics
 * where "R" is the type of the record.
 * This assumes that the Dataset has a single outer Function that we can
 * iterate over.
 * This will not work with nested Functions, for now.
 */
abstract class IterativeAdapter3[R](model: Dataset3, config: AdapterConfig) 
  extends Adapter3(model, config) {
  
  def getRecordIterator: Iterator[R] 
  
  def parseRecord(record: R): Option[Map[String, Data]]
  
  override def makeFunction(function: Function3): Option[Function3] = Option {
    // Given the cache usage, we can reuse the same sample instance.
    val sample = makeSample(function.domain, function.codomain)
    //TODO: if sample is None
    
    new Function3(function.id, function.metadata, function.domain, function.codomain)
    with SampledFunction3 {
      def iterator: Iterator[(Variable3, Variable3)] = getRecordIterator.flatMap { record =>
        parseRecord(record).flatMap { dataMap =>
          clearCache
          cache(dataMap)
          sample
        }
      }
    }
  }
  
}
