package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.{DoubleValue,StringValue}
import latis.util.StringUtils

case class StringSeqData(ss: immutable.Seq[String], textLength: Int) extends SeqData {
  //TODO: what if textLength < the default = 4?
  
  override def getByteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(size)
    //Fix length of strings
    val ss2 = ss.map(StringUtils.padOrTruncate(_, textLength))
    val cb = ss2.foldLeft(bb.asCharBuffer())(_.put(_)).rewind
    bb
  }
  
  override def length = ss.length //number of samples

  def recordSize = textLength * 2 //2 bytes per char
  
  def iterator = ss.iterator.map(s => StringValue(StringUtils.padOrTruncate(s, textLength)))
  
  def apply(index: Int) = StringValue(ss(index))
}
