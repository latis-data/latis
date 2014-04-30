package latis.data

import java.nio.ByteBuffer

object EmptyData extends SampledData {
  override def length = 0
  override def recordSize = 0
  def iterator = Iterator.empty
  override def getByteBuffer: ByteBuffer = ByteBuffer.allocate(0)
}