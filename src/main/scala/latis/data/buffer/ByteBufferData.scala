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
  

  def apply(index: Int): Data = {
    //TODO: better way to get a slice of the ByteBuffer?
    //TODO: be mindful of mutability
    val range = index * recordSize until (index + 1) * recordSize
    val bytes = range.map(buffer.get(_)).toArray
    Data(bytes)
  }
    
  
  def iterator = new Iterator[Data] {
    //Uses absolute get so buffer isn't modified
    private var _index: Int = 0
    
    override def hasNext = _index < ByteBufferData.this.length
    
    override def next = {
      _index += 1
      ByteBufferData.this(_index - 1)
    }
  }
  
  def recordSize = recSize
  
  def length = {
    if (recordSize == 0) 0
    else buffer.limit / recordSize //TODO: consider incomplete final record, truncate to int, drop incomplete record?
  }
}
