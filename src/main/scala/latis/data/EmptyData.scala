package latis.data

import java.nio.ByteBuffer

object EmptyData extends SampledData {
  override def length: Int = 0
  override def recordSize: Int = 0
  override def iterator: Iterator[SampleData] = Iterator.empty
  override def getByteBuffer: ByteBuffer = ByteBuffer.allocate(0)
}