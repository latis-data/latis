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
  
  def parseRecord(record: R): Option[Map[String, Any]]
  
  override def makeFunction(fmd: FunctionMetadata): Option[Function3] = Option {    
    val samples = getRecordIterator.flatMap { record =>
      parseRecord(record).flatMap { dm =>
        dataMap = dm
        makeSample(fmd.domain, fmd.codomain)
      }
    }

    SampledFunction3(samples)(fmd)
  }
  
  override def makeScalar(smd: ScalarMetadata): Option[Scalar3[_]] = {
//TODO: use lazy val or def to construct scalar?
    dataMap.get(smd.getId).map(Scalar3(_)(smd))
  }
//TODO: mutable so we don't risk maps in closures lingering in memory?
//but consider concurrency, avoid shared mutable state
  private var dataMap: Map[String, Any] = null
}
