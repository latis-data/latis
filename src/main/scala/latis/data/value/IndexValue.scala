package latis.data.value

import latis.data.NumberData

import java.nio.ByteBuffer

/**
 * Data implementation for a single Int value.
 */
case class IndexValue(val value: Int) extends AnyVal with NumberData {
  def size: Int = 4

  def intValue: Int = value
  def longValue: Long = value.toLong
  def floatValue: Float = value.toFloat
  def doubleValue: Double = value.toDouble

  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putInt(value).rewind.asInstanceOf[ByteBuffer]
}
