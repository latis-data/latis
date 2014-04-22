package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.DoubleValue
import latis.data.value.LongValue

case class LongSeqData(ds: immutable.Seq[Long]) extends SeqData {
  
  override def getByteBuffer: ByteBuffer = ds.foldLeft(ByteBuffer.allocate(size))(_.putLong(_)).rewind.asInstanceOf[ByteBuffer]
  
  override def length = ds.length
  def recordSize = 8
  
  def iterator = ds.iterator.map(LongValue(_))
  
  def apply(index: Int) = LongValue(ds(index))
}
