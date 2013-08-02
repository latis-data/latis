package latis.data

import java.nio.ByteBuffer

//TODO: test if we are getting the benefit of value classes

case class DoubleValue(val value: Double) extends AnyVal with NumberData {
  def length = 1
  def recordSize = 8
  
  def getDouble = Some(value)
  def getString = Some(value.toString)
  
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(recordSize).putDouble(doubleValue).flip.asInstanceOf[ByteBuffer]
  
  def iterator = List(this).iterator
}

//case class LongValue(val value: Long) extends AnyVal with NumberData {
//  def getDouble = value.toDouble
//  override def toLong = value //avoid cast to double and back
//  def length = 1
//  def recordSize = 8
//  override def toString = value.toString
//  def iterator = List(this).iterator
//}

case class StringValue(val value: String) extends AnyVal with Data {
  //TODO: treat as Array of type Char? Word = Char(4)
  def length = 1
  def recordSize = 2 * value.length //2 bytes per char
  
  def getDouble = Some(Double.NaN) //TODO: try converting String to double?
  def getString = Some(value)

  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(recordSize).putDouble(doubleValue).flip.asInstanceOf[ByteBuffer]
  
  def iterator = List(this).iterator
}
