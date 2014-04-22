package latis.data.value

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.TextData
import latis.dm.Text

//Note: beware, scala has a StringValue?

case class StringValue(val value: String) extends AnyVal with TextData {
  def size = 2 * value.length
  def isEmpty = false
  
  def stringValue = value

  def getByteBuffer: ByteBuffer = {
    value.foldLeft(ByteBuffer.allocate(size))(_.putChar(_))
    val bb = ByteBuffer.allocate(size)
    bb.asCharBuffer.put(value).rewind
    bb
  }

}
