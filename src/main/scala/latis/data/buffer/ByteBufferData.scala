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
    case _ => Some(Double.NaN) //TODO: or None?
  }
  
  def getString: Option[String] = {
    //If length = 1 (one record) return it as a string
    if (length == 1) Some(buffer.asCharBuffer().toString) 
    else None
  }
  /*
   * TODO: how should this behave? no info on how long a string can be
   * just interpret the whole record as a string?
   * if not a single string (e.g. multiple records) None or ""? vs NaN
   *   "" doesn't have not-a-string semantics
   * or error if trying to get single datum from data?
   */
  
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
  
  def length = {
    if (recordSize == 0) 0
    else buffer.limit / recordSize //TODO: consider incomplete final record, truncate to int, drop incomplete record?
  }
}

  
  /*
   * TODO:
   * diff subtypes for numeric, text, mixed?
   * longValue, charValue, stringValue(length),...?
   * getNextDouble,...?
   * getDouble(index),...?
   * how many of these should be part of Data or Number or TextData?
   */
