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
//TODO: take number of samples instead of record size?
  
  override def getByteBuffer = buffer
  
  //TODO: support getDouble, advance position?
  
  def iterator = new Iterator[Data] {
    //Uses absolute get so buffer isn't modified
    private var _index = 0
    
    override def hasNext = _index < ByteBufferData.this.length
    
    override def next = {
      val range = _index * recordSize until (_index + 1) * recordSize
      val bytes = range.map(buffer.get(_)).toArray
      _index += 1
      Data(bytes)
    }
  }
  
  def recordSize = recSize
  
  def length = {
    if (recordSize == 0) 0
    else buffer.limit / recordSize //TODO: consider incomplete final record, truncate to int, drop incomplete record?
  }
}
