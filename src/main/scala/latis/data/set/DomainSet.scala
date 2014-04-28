package latis.data.set

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.IterableData

class DomainSet extends IterableData {

  def apply(index: Int): Data = ???
  
  def indexOf(data: Data): Int = ???
  
  private var _iterableData: IterableData = null
  def recordSize: Int = _iterableData.recordSize
  
  def iterator: Iterator[Data] = _iterableData.iterator //TODO: error if null
}

object DomainSet {
  
  def apply(data: IterableData) = {
    val ds = new DomainSet
    ds._iterableData = data
    ds
  }
}