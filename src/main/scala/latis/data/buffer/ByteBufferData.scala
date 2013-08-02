package latis.data.buffer

import java.nio.ByteBuffer
import latis.data.value.DoubleValue
import latis.data.Data

/**
 * Implement Data with a ByteBuffer.
 * A record size is required for iteration.
 */
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
