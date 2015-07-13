package latis.reader.tsml

import latis.dm.Dataset
import latis.ops.Operation
import latis.reader.DatasetAccessor
import latis.reader.tsml.ml.Tsml

import java.net.URL

import scala.collection.Seq

/**
 * A Reader for a Dataset defined as a "dataset" element within TSML.
 * If the TSML URL ends with "#label", use that to reference a nested 
 * dataset element with that name attribute. 
 */
class TsmlReader(tsml: Tsml) extends DatasetAccessor {
  
  /**
   * The adapter as defined in the TSML for reading the Dataset.
   */
  lazy val adapter: TsmlAdapter = TsmlAdapter(tsml)
  
  /**
   * Return the LaTiS Dataset that TSML represents.
   */
  //TODO: careful, need to make sure kids can override without lazy val init problems, just use the ops call for now
  def getDataset: Dataset = getDataset(Seq[Operation]())
  
  //TODO: what happens if called multiple times for the same reader?
  def getDataset(operations: Seq[Operation]): Dataset = {
    adapter.getDataset(operations)
  }
  
  /**
   * Clean up any resources that the reader used.
   */
  def close = adapter.close
  
}

object TsmlReader {
  
  def apply(tsml: Tsml) = new TsmlReader(tsml)
  
  def apply(url: URL) = new TsmlReader(Tsml(url))

  def apply(path: String) = new TsmlReader(Tsml(path))
  
}







