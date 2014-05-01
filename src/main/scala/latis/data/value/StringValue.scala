package latis.data.value

import latis.data.TextData

import java.nio.ByteBuffer


/**
 * Data implementation for a single String value.
 */
case class StringValue(val value: String) extends AnyVal with TextData {
  //Note: beware, scala has a StringValue
  
  def size = 2 * value.length
  def isEmpty = false
  
  def stringValue = value

  def getByteBuffer: ByteBuffer = value.foldLeft(ByteBuffer.allocate(size))(_.putChar(_))

}
