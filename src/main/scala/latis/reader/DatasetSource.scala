package latis.reader

import latis.dm._
import latis.ops._
import latis.reader.tsml.TsmlReader3

//v3 replacement for DatasetAccessor
abstract class DatasetSource {
    
  /**
   * Return the Dataset that this source is responsible for.
   * This may involve considerable construction.
   */
  def getDataset(): Dataset3 = getDataset(Seq[Operation]())
  
  /**
   * Return the Dataset with the given Operations applied to it.
   */
  def getDataset(operations: Seq[Operation]): Dataset3
}

object DatasetSource {
  
  def fromName(dsName: String): DatasetSource = {
    TsmlReader3.fromName(dsName)
  }
}