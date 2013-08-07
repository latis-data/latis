package latis.data.value

import java.nio.ByteBuffer
import latis.data.NumberData
import latis.data.Data

//TODO: test if we are getting the benefit of value classes

case class IndexValue(val value: Int) extends AnyVal with NumberData { 
  def length = 1
  def recordSize = 4
  
  def intValue = value
  def longValue = value.toLong
  def floatValue = value.toFloat
  def doubleValue = value.toDouble
  
  //TODO: we shouldn't need to use this?
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(recordSize).putInt(value).rewind.asInstanceOf[ByteBuffer]
  
  def iterator = List(this).iterator

    //TODO: abstract up for all value classes
  def apply(index: Int): Data = index match {
    case 0 => this
    case _ => throw new IndexOutOfBoundsException()
  }
}
