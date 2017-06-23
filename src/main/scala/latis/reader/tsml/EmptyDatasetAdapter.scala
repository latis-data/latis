package latis.reader.tsml

import latis.dm.Dataset
import latis.reader.tsml.ml.Tsml

/**
 * Define a Dataset in tsml but don't back it with any data.
 * This generally assumes a Function with its type specified but no samples.
 */
class EmptyDatasetAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  /**
   * Override to return the "origDataset" which results from the first 
   * building pass instead of going on to construct the Variables with Data.
   */
  override def makeDataset(ds: Dataset): Dataset = ds
  
  /**
   * Nothing to close.
   */
  def close: Unit = {}
}