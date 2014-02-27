package latis.data.value

import latis.data.Data
import java.nio.ByteBuffer
import latis.data.TextData
import latis.dm.Text

//TODO: beware, scala has a StringValue?

case class StringValue(val value: String) extends AnyVal with TextData {
  //TODO: need to be able to specify max length for buffer size, problem for column oriented data
  //  can't have 2nd value in value class
  //Should we ignore padding in comparisons? use =~?
  
  //TODO: treat as Array of type Char? Word = Char(4)
  def length = 1 //one record //TODO: better name
  //make sure it is big enough for default size = 4 char
  //TODO: Need to pad value also?
  def recordSize = 2 * Math.max(value.length, Text.DEFAULT_LENGTH) //2 bytes per char
  
  def stringValue = value

  def getByteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(recordSize)
    bb.asCharBuffer.put(value).rewind
    bb
  }
  
  def iterator = List(this).iterator
  
    //TODO: abstract up for all value classes
  def apply(index: Int): Data = index match {
    case 0 => this
    case _ => throw new IndexOutOfBoundsException()
  }
  
}
