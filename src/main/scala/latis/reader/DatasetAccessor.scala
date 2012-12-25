package latis.reader

import latis.dm._

/**
 * Base for classes that provide data access for a Dataset.
 * A DatasetAccessor is responsible for constructing a Dataset
 * and returning data for the Variables it contains.
 */
abstract class DatasetAccessor {

  /**
   * Return the Dataset that this accessor is responsible for.
   * This may involve considerable construction.
   */
  def getDataset(): Dataset
  
  /**
   * Return the value of the datum represented by the given Scalar.
   */
  def getValue(v: Scalar[_]): Option[_]
  
  /**
   * Return an Iterator for Function samples as domain,range Pairs.
   */
  def getIterator(function: Function): FunctionIterator
  
  
  /**
   * Release any resources that this accessor acquired.
   */
  def close()
}