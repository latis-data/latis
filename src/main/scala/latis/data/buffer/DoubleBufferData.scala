package latis.data.buffer

import java.nio.ByteBuffer
import latis.data.Data
import latis.data.value.DoubleValue

/**
 * ByteBufferData where every sample/record is a single Double.
 */
class DoubleBufferData(buffer: ByteBuffer) extends ByteBufferData(buffer, 8) {
  //TODO: do we have a use case for this?
  
  override def iterator = new Iterator[Data] {
    val dbuf = buffer.asDoubleBuffer()
    override def hasNext = dbuf.hasRemaining() 
    override def next = DoubleValue(dbuf.get())
  }
}
