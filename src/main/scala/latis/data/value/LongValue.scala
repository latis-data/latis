package latis.data.value

import latis.data.NumberData

import java.nio.ByteBuffer

/**
 * Data implementation for a single Long value.
 */
case class LongValue(val value: Long) extends AnyVal with NumberData {
  def size = 8
  def isEmpty = false
  
  def intValue = value.toInt
  def longValue = value
  def floatValue = value.toFloat
  def doubleValue = value.toDouble
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putLong(value).rewind.asInstanceOf[ByteBuffer]
}
