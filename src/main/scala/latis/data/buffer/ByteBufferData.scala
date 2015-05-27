package latis.data.buffer

import latis.data.Data

import java.nio.ByteBuffer

/**
 * Implement Data with a ByteBuffer.
 */
class ByteBufferData(val buffer: ByteBuffer) extends Data {
  
  /**
   * Define the size as the capacity of the buffer even though the "limit" (actual end of the data)
   * may be smaller. This is akin to a Text variable that may be padded or truncated to a max length.
   */
  def size = buffer.capacity
  
  
  /**
   * Return a duplicate that is backed by the same data but has independent position.
   */
  def getByteBuffer = buffer.duplicate
}

object ByteBufferData {
  
  def apply(buffer: ByteBuffer): ByteBufferData = new ByteBufferData(buffer)
  
  def apply(bytes: Array[Byte]): ByteBufferData = new ByteBufferData(ByteBuffer.wrap(bytes))
}
