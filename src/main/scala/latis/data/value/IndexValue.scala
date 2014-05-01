package latis.data.value

import latis.data.NumberData

import java.nio.ByteBuffer

/**
 * Data implementation for a single Int value.
 */
case class IndexValue(val value: Int) extends AnyVal with NumberData { 
  def size = 4
  def isEmpty = false
  
  def intValue = value
  def longValue = value.toLong
  def floatValue = value.toFloat
  def doubleValue = value.toDouble
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putInt(value).rewind.asInstanceOf[ByteBuffer]
}
