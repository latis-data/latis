package latis.data.set

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.IterableData

abstract class DomainSet extends IterableData {

  def apply(index: Int): Data
  
  def indexOf(data: Data): Int
  
  def recordSize: Int
  
  def iterator: Iterator[Data]
}

object DomainSet {
  
  def apply(data: IterableData) = new DomainSet {
    def apply(index: Int): Data = ???
    def indexOf(data: Data): Int = ???
   
    def recordSize: Int = data.recordSize
    def iterator: Iterator[Data] = data.iterator
  }
}