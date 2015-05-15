package latis.util.iterator

import java.nio.ByteBuffer
import latis.dm.Variable

/**
 * Makes a PeekIterator of Array[Byte] from a ByteBuffer. 
 * The buffer will be broken into Arrays of length 'size'.
 */
class BufferIterator(buffer: ByteBuffer, size: Int) extends PeekIterator[Array[Byte]]{
  
  final protected def getNext: Array[Byte] = {
    if (! buffer.hasRemaining) null
    else {
      val arr = Array.ofDim[Byte](size)
      buffer.get(arr)
      arr
    }
  }

}

object BufferIterator {
  def apply(buffer: ByteBuffer, size: Int) = new BufferIterator(buffer, size)
  def apply(buffer: ByteBuffer, v: Variable) = new BufferIterator(buffer, v.getSize)
}