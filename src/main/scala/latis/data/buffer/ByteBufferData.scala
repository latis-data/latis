package latis.data.buffer

import latis.data.Data

import java.nio.ByteBuffer

/**
 * Implement Data with a ByteBuffer.
 */
class ByteBufferData(val buffer: ByteBuffer) extends Data {
  
  def size = buffer.limit //TODO: or limit-position since that is all it currently has to offer? and how ByteBuffer.equals works
  
  /**
   * Return a duplcate that is backed by the same data but has independent position.
   */
  def getByteBuffer = buffer.duplicate
}
