package latis.data.value

import latis.data.TextData

import java.nio.ByteBuffer


/**
 * Data implementation for a single String value.
 */
case class StringValue(val value: String) extends AnyVal with TextData {
  //Note: beware, scala has a StringValue

  def size: Int = 2 * value.length

  def stringValue: String = value

  def getByteBuffer: ByteBuffer = value.foldLeft(ByteBuffer.allocate(size))(_.putChar(_)).flip.asInstanceOf[ByteBuffer]

}
