package latis.data

import java.nio.ByteBuffer
import latis.data.value.IndexValue

class IndexData(val start: Int, val stop: Int, val stride: Int) extends Data {
  
  def length: Int = (stop - start) / stride + 1
  
  //TODO: Index values are not to be serialized, so return empty?
  def recordSize: Int = 0
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(0)
  
  def iterator = new Iterator[Data]() {
    var index = start
  
    def hasNext(): Boolean = index + stride < length
  
    def next() = {
      index += stride
      IndexValue(index)
    }
  }
  
  
  override def equals(that: Any) = that match {
    case d: IndexData => d.start == start && d.stop == stop && d.stride == stride
    case _ => false
  }
  
  override def hashCode = start + 7 * stop + 17 * stride
}


object IndexData {
  //TODO: n-dimensions
  
  def apply(start: Int, stop: Int, stride: Int): IndexData = new IndexData(start, stop, stride)
  
  def apply(start: Int, stop: Int): IndexData = IndexData(start, stop, 1)
  
  def apply(length: Int): IndexData = IndexData(0, length-1, 1)
}