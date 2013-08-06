package latis.dm

import latis.data._
import latis.data.value.IndexValue

/**
 * Scalar Variable that represents an index.
 * Implemented such that its Data represents the index values.
 */
class Index extends Scalar with Number {
  /*
   * try to impl Data with index math - IndexData
   * 
   * What about IndexData vs IndexValue?
   * pattern match? but don't want to instantiate value class
   * same issue with other scalars
   * 
   */
  
  //needed for Number
  def doubleValue = data.asInstanceOf[NumberData].doubleValue
}


object Index {
  
  def apply(): Index = new Index //(EmptyMetadata, EmptyData)
  
  def apply(length: Int): Index = {
    val index = Index()
    index._data = IndexData(length)
    index
  }  
  
  //TODO: disambiguate from length (long?)
//  def apply(value: Int): Index = {
//    val index = Index()
//    index._data = ???
//    index
//  }
  
  //def unapply
}