package latis.reader.tsml

import latis.dm._
import latis.data.Data
import latis.reader.tsml.ml.FunctionMl
import latis.reader.tsml.ml.Tsml

/**
 * This Adapter is designed for arbitrarily large Datasets that can be
 * processed one sample at a time. The Data will be managed in the Function
 * via the Data's iterator which can be fed by this Adapter.
 */
abstract class IterativeAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  //TODO: Stream, lazy list
  //TODO: consider Iterative and Granule as traits?
  
  //TODO: be lazy, don't access data source until Iterator is used
  //  can't wail till "next"
  //  wrap Data with function that makes Iterator, call it when invoking data.iterator
  
  //TODO: assuming single top level variable = Function
  
  //TODO: move to Function?
  lazy val sampleSize = {
    val f = dataset(0).asInstanceOf[Function] //assume single top-level Function
    f.getDomain.getSize + f.getRange.getSize
  }
  
  def makeIterableData(sampleTemplate: Sample): Data
  //note, Function has dataToSample, the inverse of this, except this sample has no data
  
  override def makeFunction(fml: FunctionMl): Option[Function] = {
    val md = makeMetadata(fml)
    
    //if domain or range is None (e.g. not projected), make index function
    val domain = makeVariable(fml.domain)
    val range = makeVariable(fml.range)
    val sample = (domain, range) match {
      case (Some(d), Some(r)) => Sample(d,r)
      case (None, Some(r))    => Sample(Index(-1), r) //TODO: do we need a valid value here? Sample is used just as a template
      case (Some(d), None)    => Sample(Index(-1), d) //no range, so make domain the range of an index function
      case (None, None) => ??? //TODO: nothing projected
    }
    
    val data: Data = makeIterableData(sample)
    
    Some(Function(sample.domain, sample.range, md, data))
  }
  
}