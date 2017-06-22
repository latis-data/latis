package latis.data.value

import latis.data.NumberData

import java.nio.ByteBuffer

/**
 * Data implementation for a single Double value.
 */
case class DoubleValue(val value: Double) extends AnyVal with NumberData {
  //TODO: test if we are getting the benefit of value classes
  //  http://docs.scala-lang.org/overviews/core/value-classes.html

  def size: Int = 8

  //TODO: round or truncate?
  def intValue: Int = value.toInt
  def longValue: Long = value.toLong
  def floatValue: Float = value.toFloat
  def doubleValue: Double = value

  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putDouble(value).rewind.asInstanceOf[ByteBuffer]

}
