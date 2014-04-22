package latis.data.value

import java.nio.ByteBuffer
import latis.data._

//TODO: test if we are getting the benefit of value classes
//TODO: ValueData trait?

case class LongValue(val value: Long) extends AnyVal with NumberData {
  def size = 8
  def isEmpty = false
  
  def intValue = value.toInt
  def longValue = value
  def floatValue = value.toFloat
  def doubleValue = value.toDouble
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putLong(longValue).rewind.asInstanceOf[ByteBuffer]
}
