package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.DoubleValue

case class DoubleSeqData(ds: immutable.Seq[Double]) extends SeqData {
  
  def getByteBuffer: ByteBuffer = ds.foldLeft(ByteBuffer.allocate(size))(_.putDouble(_)).rewind.asInstanceOf[ByteBuffer]
  
  def length = ds.length
  def recordSize = 8
  
  def iterator = ds.iterator.map(DoubleValue(_))
  
  def apply(index: Int) = DoubleValue(ds(index))
}
