package latis.data.buffer

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.IterableData
import latis.dm.Variable
import latis.util.BufferIterator
import latis.dm.Binary

/**
 * Implement Data with a ByteBuffer.
 */
abstract class ByteBufferData(val buffer: ByteBuffer) extends IterableData {
  
  override def size = buffer.limit //TODO: or limit-position since that is all it currently has to offer? and how ByteBuffer.equals works
  
  /**
   * Return a duplicate that is backed by the same data but has independent position.
   */
  override def getByteBuffer = buffer.duplicate
  
}

object ByteBufferData {
  
  def apply(buf: ByteBuffer) = new ByteBufferData(buf) {
    def recordSize = buf.limit
    def iterator = new BufferIterator(buf, Binary(buf))
  }
  
  def apply(buf: ByteBuffer, v: Variable) = new ByteBufferData(buf) {
    def recordSize = v.getSize
    def iterator = new BufferIterator(buf, v)
  }
  
}
