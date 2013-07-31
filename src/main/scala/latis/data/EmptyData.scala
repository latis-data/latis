package latis.data

import java.nio.ByteBuffer

object EmptyData extends Data {
  override def getByteBuffer: ByteBuffer = ByteBuffer.allocate(0)
  override def iterator = Iterator.empty
  override def getDouble = None
  override def isEmpty = true
  override def length = 0
  override def recordSize = 0
  //TODO: getString? ""? error?
}