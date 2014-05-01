package latis.data.value

import latis.data.NumberData

import java.nio.ByteBuffer

/**
 * Data implementation for a single Double value.
 */
case class DoubleValue(val value: Double) extends AnyVal with NumberData {
  //TODO: test if we are getting the benefit of value classes
  //  http://docs.scala-lang.org/overviews/core/value-classes.html
  
  def size = 8
  
  //TODO: round or truncate?
  def intValue = value.toInt
  def longValue = value.toLong
  def floatValue = value.toFloat
  def doubleValue = value
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putDouble(value).rewind.asInstanceOf[ByteBuffer]

}
