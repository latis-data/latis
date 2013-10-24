package latis.dm

import latis.data._
import latis.data.value.IndexValue
import latis.metadata.Metadata

/**
 * Scalar Variable that represents an index.
 * Implemented such that its Data represents the index values.
 */
trait Index extends Scalar with Number {
  //def value: Int = intValue
  //def stringToValue(s: String): Int = s.toInt
  //def compare(that: String): Int = intValue compare that.toInt
  /*
   * try to impl Data with index math - IndexData
   * 
   * What about IndexData vs IndexValue?
   * pattern match? but don't want to instantiate value class
   * same issue with other scalars
   * Index is a single model component, Index with IndexData is an implicit Function
   */
  
  //needed for Number
  //def doubleValue = getData.asInstanceOf[NumberData].doubleValue
}


object Index {
  
  def apply(): Index = new Variable2(metadata = Metadata("index")) with Index
  
  def withLength(length: Int): Index = new Variable2(data = IndexData(length)) with Index

  def apply(value: Int): Index = new Variable2(data = IndexValue(value)) with Index

  
  def unapply(index: Index): Option[Int] = Some(index.getData.asInstanceOf[IndexValue].intValue)
  //TODO: make sure we have a data value as opposed to IndexData...
}