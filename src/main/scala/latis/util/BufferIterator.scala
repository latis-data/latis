package latis.util

import java.nio.ByteBuffer
import java.nio.charset.Charset

import latis.data.Data
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text
import latis.dm.Variable

/**
 * Makes a PeekIterator of Data from a ByteBuffer. 
 * A template Variable is required to determine how to segment the buffer.
 */
class BufferIterator(buffer: ByteBuffer, v: Variable) extends PeekIterator[Data]{
  
  final protected def getNext: Data = {
    if (! buffer.hasRemaining) null
    else {
      try v match {
        case i: Integer => Data(buffer.getLong)
        case r: Real => Data(buffer.getDouble)
        case t: Text => {
          val arr = Array.ofDim[Byte](t.getSize)
          buffer.get(arr)
          val s = StringUtils.padOrTruncate(new String(arr.filter(_!=0), Charset.forName("ISO_8859_1")), t.length)
          Data(s)
        }
//      } catch {
//        case e: Exception => getNext
      }
    }
  }

}