package latis.data.set

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.IterableData

abstract class DomainSet extends IterableData {

  def apply(index: Int): Data
  
  def indexOf(data: Data): Int
  
  def getByteBuffer: ByteBuffer 
  
  def isEmpty: Boolean
  def recordSize: Int
  //override def length: Int =
  
  def iterator: Iterator[Data]
}