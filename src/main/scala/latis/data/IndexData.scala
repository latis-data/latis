package latis.data

import java.nio.ByteBuffer
import latis.data.value.IndexValue

class IndexData(val start: Int, val stop: Int, val stride: Int) extends IterableData {
  
  override def length: Int = (stop - start) / stride + 1
  def recordSize: Int = 4
  //def getByteBuffer: ByteBuffer = ByteBuffer.allocate(size)
  
  def iterator = new Iterator[Data]() {
    var index = start
  
    def hasNext(): Boolean = index + stride < IndexData.this.length
  
    def next() = {
      index += stride
      IndexValue(index)
    }
  }
  
  def apply(index: Int): Data = IndexValue(start + stride * index)
  
  
  override def equals(that: Any) = that match {
    case d: IndexData => d.start == start && d.stop == stop && d.stride == stride
    case _ => false
  }
  
  override def hashCode = start + 7 * stop + 17 * stride
}


object IndexData {
  //TODO: n-dimensions
  //TODO: error if start > stop...
  
  def apply(start: Int, stop: Int, stride: Int): IndexData = new IndexData(start, stop, stride)
  
  def apply(start: Int, stop: Int): IndexData = IndexData(start, stop, 1)
  
  def apply(length: Int): IndexData = IndexData(0, length-1, 1)
}