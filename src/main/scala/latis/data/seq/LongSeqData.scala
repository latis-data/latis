package latis.data.seq

import latis.data.IterableData
import latis.data.value.LongValue

import java.nio.ByteBuffer

import scala.collection.immutable

case class LongSeqData(ds: immutable.Seq[Long]) extends IterableData {
  
  override def getByteBuffer: ByteBuffer = ds.foldLeft(ByteBuffer.allocate(size))(_.putLong(_)).rewind.asInstanceOf[ByteBuffer]
  
  override def length: Int = ds.length
  def recordSize: Int = 8
  
  def iterator: Iterator[LongValue] = ds.iterator.map(LongValue(_))
  
  def apply(index: Int): LongValue = LongValue(ds(index))
}
