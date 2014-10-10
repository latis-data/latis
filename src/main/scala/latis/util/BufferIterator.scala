package latis.util

import java.nio.ByteBuffer
import scala.annotation.tailrec
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Variable
import latis.data.Data

class BufferIterator(buffer: ByteBuffer, v: Variable) extends PeekIterator[Data]{
  
  @tailrec final protected def getNext: Data = {
    if (! buffer.hasRemaining) null
    else {
      try v match {
        case i: Integer => Data(buffer.getLong)
        case r: Real => Data(buffer.getDouble)
        //TODO: other types
      } catch {
        case e: Exception => getNext
      }
    }
  }

}