package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.{DoubleValue,StringValue}

case class StringSeqData(ss: immutable.Seq[String]) extends SeqData {
  
  def getByteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(size)
    val cb = ss.foldLeft(bb.asCharBuffer())(_.put(_)).rewind
    bb
  }
  
  def length = ss.length
  
  /**
   * Record size must be fixed, so for a sequence of Strings, use the max String.
   */
  def recordSize = ss.map(_.length).max * 2 //2 bytes per char
 //TODO: need to use size defined in the Text variable?
  
  def iterator = ss.iterator.map(StringValue(_))
  
  def apply(index: Int) = StringValue(ss(index))
}
