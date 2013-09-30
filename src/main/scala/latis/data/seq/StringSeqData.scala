package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.{DoubleValue,StringValue}

case class StringSeqData(ss: immutable.Seq[String], textLength: Int) extends SeqData {
  
  def getByteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(size)
    //Need to have exactly 'textLength' characters in the buffer.
    //If string value is too long, drop extra characters.
    //If too short, pad with spaces. TODO: right or left padding? right, for now
    val ss2 = ss.map(padOrTruncate(_, textLength))
    val cb = ss2.foldLeft(bb.asCharBuffer())(_.put(_)).rewind
    bb
  }
  
  //TODO: util
  def padOrTruncate(s: String, length: Int): String = s.length match {
    case l: Int if (l < length) => s.padTo(length, ' ')
    case l: Int if (l > length) => s.substring(0, length)
    //otherwise, the size is just right
    case _ => s
  }
  
  def length = ss.length //number of samples
  
  /**
   * Record size must be fixed, so for a sequence of Strings, use the max String.
   */
  //def recordSize = ss.map(_.length).max * 2 //2 bytes per char
  //need to use size defined in the Text variable
  def recordSize = textLength * 2 //2 bytes per char
  
  def iterator = ss.iterator.map(s => StringValue(padOrTruncate(s, textLength)))
  
  def apply(index: Int) = StringValue(ss(index))
}
