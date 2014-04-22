package latis.data

import java.nio.ByteBuffer

object EmptyData extends IterableData {
  override def getByteBuffer: ByteBuffer = ByteBuffer.allocate(0)
  def iterator = Iterator.empty
  override def length = 0
  def recordSize = 0
  
  //def apply(index: Int): Data = throw new RuntimeException("EmptyData")
}