package latis.data

import java.nio.ByteBuffer

//TODO: test if we are getting the benefit of value classes

case class DoubleValue(val value: Double) extends AnyVal with NumberData {
  override def doubleValue = value
  //def length = 1
  //def recordSize = 8
  override def toString = value.toString
  //def iterator = List(this).iterator
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
  override def toString = value
  override def recordSize = 2 * value.length //2 bytes per char
  override def iterator = List(this).iterator
}
