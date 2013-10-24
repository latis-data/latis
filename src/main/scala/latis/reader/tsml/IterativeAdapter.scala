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
    
 //TODO: if domain or range is None (e.g. projection), make index function
    
    val domain = makeVariable(fml.domain).get //TODO: better use of Option
    val range = makeVariable(fml.range).get
    //val data = for (domain <- makeVariable(fml.domain); range <- makeVariable(fml.range)) 
    //  yield makeIterableData(Sample(domain, range))
    
    val data: Data = makeIterableData(Sample(domain, range))
    
    Some(Function(domain, range, md, data))
  }
  
}