package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.{DoubleValue,StringValue}

case class StringSeqData(ss: immutable.Seq[String], textLength: Int) extends SeqData {
  
  def getByteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(size)
    //TODO: cut off anything beyond size/2 characters
    //TODO: pad?
    val cb = ss.foldLeft(bb.asCharBuffer())(_.put(_)).rewind
    bb
  }
  
  def length = ss.length //number of samples
  
  /**
   * Record size must be fixed, so for a sequence of Strings, use the max String.
   */
  //def recordSize = ss.map(_.length).max * 2 //2 bytes per char
  //need to use size defined in the Text variable
  def recordSize = textLength * 2 //2 bytes per char
  
  def iterator = ss.iterator.map(StringValue(_))
  
  def apply(index: Int) = StringValue(ss(index))
}
