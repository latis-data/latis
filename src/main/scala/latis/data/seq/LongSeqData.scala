package latis.data.seq

import latis.data.value.LongValue

import java.nio.ByteBuffer

import scala.collection.immutable

case class LongSeqData(ds: immutable.Seq[Long]) extends SeqData {
  
  override def getByteBuffer: ByteBuffer = ds.foldLeft(ByteBuffer.allocate(size))(_.putLong(_)).rewind.asInstanceOf[ByteBuffer]
  
  override def length = ds.length
  def recordSize = 8
  
  def iterator = ds.iterator.map(LongValue(_))
  
  def apply(index: Int) = LongValue(ds(index))
}
