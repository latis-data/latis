package latis.dm

import latis.data._
import latis.data.value.IndexValue
import latis.metadata.Metadata

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
   * Index is a single model component, Index with IndexData is an implicit Function
   */
  
  //needed for Number
  def doubleValue = data.asInstanceOf[NumberData].doubleValue
}


object Index {
  
  def apply(): Index = {
    val index = new Index //(EmptyMetadata, EmptyData)
    index._metadata = Metadata("index") //set name metadata to "index"
    index
  }
  
  def withLength(length: Int): Index = {
    val index = Index()
    index._data = IndexData(length)
    index
  }  

  def apply(value: Int): Index = {
    val index = Index()
    index._data = IndexValue(value)
    index
  }
  
  def unapply(index: Index): Option[Int] = Some(index.data.asInstanceOf[IndexValue].intValue)
  //TODO: make sure we have a data value as opposed to IndexData...
}