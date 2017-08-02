package latis.reader.adapter

import latis.data._
import latis.dm._
import scala.collection._

/**
 * Base class for Adapters for data sources that have 'record' semantics.
 * "R" is the type of the record.
 * This assumes that the Dataset has a single outer Function that we can
 * iterate over with no nested Functions.
 */
abstract class IterativeAdapter3[R](model: Model, properties: Map[String, String]) 
  extends Adapter(model, properties) {
  //Note: IterativeAdapter2 supports nested Functions by using 
  //  DataMapUtils.dataMapsToFunction. This one does not.
  
  def getRecordIterator: Iterator[R] 
  
  def parseRecord(record: R): Option[Map[String, Data]]
  
  override def makeFunction(f: FunctionType): Option[Function] = {
    val samples = getRecordIterator.flatMap {
      parseRecord(_).flatMap { dataMap =>
        clearCache
        cache(dataMap)
        for {
          d <- makeVariable(f.domain);
          r <- makeVariable(f.codomain)
        } yield Sample(d, r)
      }
    }
    
    /*
     * TODO: Don't peek into Iterator.
     * The Function construction requires Variables for the domain and range.
     * Note that "Naught" did not work (e.g. for projections).
     * We should refactor Function to take VariableTypes instead.
     * Note that we are not using our PeekIterator.
     */
    //make BufferedIterator so we can peek at the first sample
    //TODO: may initiate data reading
    if (samples.hasNext) {
      val bufferedSamples = samples.buffered
      val smp = bufferedSamples.head
      Option(SampledFunction(smp.domain, smp.range, bufferedSamples, f.metadata))
    } else None
  }
  
}
