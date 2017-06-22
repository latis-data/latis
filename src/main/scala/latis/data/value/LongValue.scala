package latis.data.value

import latis.data.NumberData

import java.nio.ByteBuffer

/**
 * Data implementation for a single Long value.
 */
case class LongValue(val value: Long) extends AnyVal with NumberData {
  def size: Int = 8

  def intValue: Int = value.toInt
  def longValue: Long = value
  def floatValue: Float = value.toFloat
  def doubleValue: Double = value.toDouble

  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putLong(value).rewind.asInstanceOf[ByteBuffer]
}
