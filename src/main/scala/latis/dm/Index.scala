package latis.dm

import latis.data._
import latis.data.value.IndexValue
import latis.metadata.Metadata

/**
 * Scalar Variable that represents an index.
 * Implemented such that its Data represents the index values.
 */
trait Index extends Scalar with Number


object Index {
  
  def apply(): Index = new AbstractScalar(metadata = Metadata("index")) with Index

  def apply(value: Int): Index = new AbstractScalar(metadata = Metadata("index"), data = IndexValue(value)) with Index

  def unapply(index: Index): Option[Int] = Some(index.getNumberData.intValue)
}