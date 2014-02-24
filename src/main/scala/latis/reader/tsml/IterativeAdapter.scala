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
  //TODO: Stream, lazy list?
  //TODO: consider Iterative and Granule as traits?
  //TODO: consider use cases beyond single top level variable = Function
  
  /**
   * Implementations of IterativeAdapter need to override this to construct Data
   * that can iterate over each sample.
   */
  def makeIterableData(sampleTemplate: Sample): Data
  
  /**
   * Override to make Function with IterableData.
   */
  override def makeFunction(f: Function): Option[Function] = {
    //if domain or range is None (e.g. not projected), make index function
    val template = Sample(f.getDomain, f.getRange)
    makeSample(template) match {
      case Some(sample) => {
        val data: Data = makeIterableData(sample)
        Some(Function(sample.domain, sample.range, f.getMetadata, data))
      }
      case None => None
    }
  }
  
}
