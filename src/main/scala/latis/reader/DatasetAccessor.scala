package latis.reader

import latis.dm.Dataset

/**
 * Trait that provide data access for a Dataset.
 * A DatasetAccessor is responsible for constructing a Dataset
 * and returning data for the Variables it contains.
 */
trait DatasetAccessor {

  /**
   * Return the Dataset that this accessor is responsible for.
   * This may involve considerable construction.
   */
  def getDataset(): Dataset
  
  
  /**
   * Release any resources that this accessor acquired.
   */
  def close()
}