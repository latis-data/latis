package latis.data.value

import latis.data.Data
import java.nio.ByteBuffer

case class StringValue(val value: String) extends AnyVal with Data {
  //TODO: treat as Array of type Char? Word = Char(4)
  def length = 1
  def recordSize = 2 * value.length //2 bytes per char
  
  def getDouble = Some(Double.NaN) //TODO: try converting String to double?
  def getString = Some(value)

  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(recordSize).putDouble(doubleValue).flip.asInstanceOf[ByteBuffer]
  
  def iterator = List(this).iterator
}
