package latis.reader.adapter

import latis.data._
import latis.dm._
import latis.metadata._
import scala.collection._

/**
 * Base class for Adapters for data sources that have 'record' semantics
 * where "R" is the type of the record.
 * This assumes that the Dataset has a single outer Function that we can
 * iterate over.
 * This will not work with nested Functions, for now.
 */
abstract class IterativeAdapter3[R](metadata: Metadata3, config: AdapterConfig) 
  extends Adapter3(metadata, config) {
  
  def getRecordIterator: Iterator[R] 
  
  def parseRecord(record: R): Option[Map[String, Data]]
  
  override def makeFunction(fmd: FunctionMetadata): Option[Function3] = Option {
    // Given the cache usage, we can reuse the same sample instance.
    val sample = makeSample(fmd.domain, fmd.codomain) match {
      case Some(s) => s
      case None => ??? //TODO empty Function?
    }
    
    new Function3(sample._1, sample._2)(fmd)
    //new Function3(function.id, function.metadata, function.domain, function.codomain)
    with SampledFunction3 {
      def iterator: Iterator[(Variable3, Variable3)] = getRecordIterator.flatMap { record =>
        parseRecord(record).map { dataMap =>
          clearCache
          cache(dataMap)
          sample
        }
      }
    }
  }
  
}
