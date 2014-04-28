package latis.data.buffer

import java.nio.ByteBuffer
import latis.data.value.DoubleValue
import latis.data.Data

/**
 * Implement Data with a ByteBuffer.
 */
class ByteBufferData(val buffer: ByteBuffer) extends Data {
  
  def size = buffer.limit //TODO: or limit-position since that is all it currently has to offer? and how ByteBuffer.equals works
  def isEmpty = size == 0
  def getByteBuffer = buffer
}
