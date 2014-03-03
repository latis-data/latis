package latis.data.value

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.TextData
import latis.dm.Text

//Note: beware, scala has a StringValue?

case class StringValue(val value: String) extends AnyVal with TextData {
  
  def length = 1 //one record //TODO: better name

  def recordSize = 2 * value.length
  
  def stringValue = value

  def getByteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(recordSize)
    bb.asCharBuffer.put(value).rewind
    bb
  }
  
  def iterator = List(this).iterator
  
  //TODO: abstract up for all value classes?
  def apply(index: Int): Data = index match {
    case 0 => this
    case _ => throw new IndexOutOfBoundsException()
  }
  
}
