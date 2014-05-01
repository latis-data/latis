package latis.data.set

import latis.data.Data
import latis.data.value.IndexValue

/**
 * One dimensional DomainSet representing index values by start, stop, and stride.
 */
class IndexSet(val start: Int, val stop: Int, val stride: Int) extends DomainSet {
  //TODO: support unlimited with current length: -n
  
  override def recordSize: Int = 4
  override def length: Int = (stop - start) / stride + 1

  def iterator = new Iterator[Data]() {
    var index = start - stride
  
    def hasNext(): Boolean = index + stride < IndexSet.this.length
  
    def next() = {
      index += stride
      IndexValue(index)
    }
  }
  
  def apply(index: Int): Data = IndexValue(start + stride * index)
    
  override def indexOf(data: Data) = data match {
    case IndexValue(i) => (i - start) / stride 
    //TODO: error if not a multiple of stride?
    //TODO: other Data that evaluates to Int
  }
  
  override def equals(that: Any) = that match {
    case d: IndexSet => d.start == start && d.stop == stop && d.stride == stride
    case _ => false
  }
  
  override def hashCode = start + 7 * stop + 17 * stride

}

object IndexSet {
    
  def apply(start: Int, stop: Int, stride: Int): IndexSet = new IndexSet(start, stop, stride)
  def apply(start: Int, stop: Int): IndexSet = IndexSet(start, stop, 1)
  def apply(length: Int): IndexSet = IndexSet(0, length-1, 1)
  def apply() = new IndexSet(0, Int.MaxValue-1, 1) //TODO: support infinite? length = -1?
  
}