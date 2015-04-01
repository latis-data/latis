package latis.data

import latis.data.buffer.ByteBufferData
import latis.data.seq.DataSeq
import latis.data.seq.DoubleSeqData
import latis.data.seq.LongSeqData
import latis.data.seq.StringSeqData
import latis.data.value.DoubleValue
import latis.data.value.LongValue
import latis.data.value.StringValue
import java.nio.ByteBuffer
import scala.Array.canBuildFrom
import scala.collection.Seq
import scala.collection.immutable
import java.nio.Buffer

/**
 * Base type for representing data values.
 */
trait Data extends Any {
  
  /**
   * Size in bytes.
   */
  def size: Int 
  
  /**
   * Return the data as a ByteBuffer.
   */
  def getByteBuffer: ByteBuffer 
  
  /**
   * Return the data as an array of bytes.
   */
  def getBytes: Array[Byte] = getByteBuffer.array
  
  /**
   * Is the Data empty.
   */
  def isEmpty: Boolean = size == 0
  
  /**
   * Are there any values represented by this Data object.
   */
  def notEmpty = ! isEmpty
  
  /**
   * Concatenate the given Data onto the end of this Data.
   */
  def concat(data: Data) = Data(this.getBytes ++ data.getBytes)

  /**
   * Debug method to see what values are in a Data object. Assumes doubles.
   */
  def writeDoubles = {
    val bb = getByteBuffer.rewind.asInstanceOf[ByteBuffer]
    val db = bb.asDoubleBuffer
    val n = db.limit
    for (i <- 0 until n) println(db.get(i))
  }
  
  /*
   * TODO: is byte buffer equality sufficient?
   * should also consider number of records
   * 
   * From javadoc:
   *   The hash code of a byte buffer depends only upon its remaining elements; 
   *   that is, upon the elements from position() up to, and including, the element at limit() - 1.
   *   
   * Note: value classes cannot override equals, so our value data will not inherit this
   */
  override def equals(that: Any) = that match {
    case d: Data => d.getByteBuffer == getByteBuffer
    case _ => false
  }
  
  override def hashCode = getByteBuffer.hashCode
}

//-----------------------------------------------------------------------------

object Data {
  
  val empty = EmptyData
  
  def apply(any: Any): Data = any match {
    case d: Double => DoubleValue(d)
    case f: Float  => DoubleValue(f) //represent floats as doubles
    case l: Long   => LongValue(l)
    case i: Int    => LongValue(i) //represent ints as longs
    case s: String => StringValue(s)
    
    case buffer: Buffer     => fromBuffer(buffer)
    case bytes: Array[Byte] => fromBuffer(ByteBuffer.wrap(bytes))
  }

  /**
   * Construct a Data implementation that encapsulates a ByteBuffer.
   * This will set the limit and rewind the buffer if needed.
   */
  def fromBuffer(buffer: Buffer): Data = {
    //If it looks like this buffer isn't set to the beginning, do so.
    //Note, 'flip' will set the limit to the current position and the position to 0
    //and leave the capacity unchanged. 
    val b = if (buffer.limit > 0 && buffer.position != 0) buffer.flip else buffer
    b match {
      case bb: ByteBuffer => new ByteBufferData(bb)
      //TODO: support other types of Buffer?
    }
  }
  
}
