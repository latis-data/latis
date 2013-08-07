package latis.data.value

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.TextData

//TODO: beware, scala has a StringValue?

case class StringValue(val value: String) extends AnyVal with TextData {
  //TODO: treat as Array of type Char? Word = Char(4)
  def length = 1
  def recordSize = 2 * value.length //2 bytes per char
  
  def stringValue = value

  def getByteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(recordSize)
    bb.asCharBuffer.put(value).rewind
    bb
  }
  
  def iterator = List(this).iterator
  
    //TODO: abstract up for all value classes
  def apply(index: Int): Data = index match {
    case 1 => this
    case _ => throw new IndexOutOfBoundsException()
  }
}
