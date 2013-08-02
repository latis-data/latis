package latis.data

import java.nio.ByteBuffer
import latis.data.value.DoubleValue

  //construct with record size so we can iterate over a sample
class ByteBufferData(val buffer: ByteBuffer, recSize: Int) extends Data {
  
  override def getByteBuffer = buffer
  
  def getDouble: Option[Double] = length match {
    case 0 => None
    case 1 => Some(buffer.getDouble(0))
    case _ => Some(Double.NaN)
  }
  
  def getString: Option[String] = ???
  
  def iterator = new Iterator[Data] {
    private var _index = 0
    
    override def hasNext = _index < ByteBufferData.this.length
    
    override def next = {
      val bb = ByteBuffer.wrap(buffer.array, _index * recordSize, recordSize)
      _index += 1
      Data(bb)
    }
  }
  
  def recordSize = recSize
  def length = buffer.limit / recordSize
}

  
  /*
   * TODO:
   * diff subtypes for numeric, text, mixed?
   * longValue, charValue, stringValue(length),...?
   * getNextDouble,...?
   * getDouble(index),...?
   * how many of these should be part of Data or Number or TextData?
   */

  //TODO: use "flip" instead of rewind to set limit to end of data instead of capacity, also sets pointer to zero

class DoubleBufferData(buffer: ByteBuffer) extends ByteBufferData(buffer, 8) {
  
  override def iterator = new Iterator[Data] {
    val dbuf = buffer.asDoubleBuffer()
    override def hasNext = dbuf.hasRemaining() 
    override def next = DoubleValue(dbuf.get())
  }
}