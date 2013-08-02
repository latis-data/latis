package latis.data.value

import java.nio.ByteBuffer
import latis.data.NumberData

//TODO: test if we are getting the benefit of value classes

case class DoubleValue(val value: Double) extends AnyVal with NumberData {
  def length = 1
  def recordSize = 8
  
  def getDouble = Some(value)
  def getString = Some(value.toString)
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(recordSize).putDouble(doubleValue).flip.asInstanceOf[ByteBuffer]
  
  def iterator = List(this).iterator
}
