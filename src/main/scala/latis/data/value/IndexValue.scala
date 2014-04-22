package latis.data.value

import java.nio.ByteBuffer
import latis.data.NumberData
import latis.data.Data

//TODO: test if we are getting the benefit of value classes

case class IndexValue(val value: Int) extends AnyVal with NumberData { 
  def size = 4
  def isEmpty = false
  
  def intValue = value
  def longValue = value.toLong
  def floatValue = value.toFloat
  def doubleValue = value.toDouble
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size).putInt(value).rewind.asInstanceOf[ByteBuffer]
}
