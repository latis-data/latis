package latis.data.seq

import latis.data.value.DoubleValue

import java.nio.ByteBuffer

import scala.collection.immutable

case class DoubleSeqData(ds: immutable.Seq[Double]) extends SeqData {
  
  override def getByteBuffer: ByteBuffer = ds.foldLeft(ByteBuffer.allocate(size))(_.putDouble(_)).rewind.asInstanceOf[ByteBuffer]
  
  override def length = ds.length
  def recordSize = 8
  
  def iterator = ds.iterator.map(DoubleValue(_))
  
  def apply(index: Int) = DoubleValue(ds(index))
}
