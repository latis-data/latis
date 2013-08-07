package latis.data.value

import java.nio.ByteBuffer
import latis.data._

//TODO: test if we are getting the benefit of value classes
//TODO: ValueData trait?

case class LongValue(val value: Long) extends AnyVal with NumberData {
  def length = 1
  def recordSize = 8
  
  //TODO: put in NumberData? Can we put the impl there and still be a value class?
  def intValue = value.toInt
  def longValue = value
  def floatValue = value.toFloat
  def doubleValue = value.toDouble
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(recordSize).putLong(longValue).rewind.asInstanceOf[ByteBuffer]
  
  def iterator = List(this).iterator

    //TODO: abstract up for all value classes
  def apply(index: Int): Data = index match {
    case 0 => this
    case _ => throw new IndexOutOfBoundsException()
  }
  
}
