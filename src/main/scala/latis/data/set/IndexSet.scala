package latis.data.set

import latis.data.value.IndexValue
import latis.data.Data

class IndexSet extends DomainSet {

  override def apply(index: Int) = IndexValue(index)
  
  override def indexOf(data: Data) = data match {
    case IndexValue(i) => i
  }

  override def iterator = new Iterator[Data]() {
    var index = -1
  
    def hasNext: Boolean = true //index + 1 < myLength
  
    def next = {
      index += 1
      IndexValue(index)
    }
  }
  
  override def recordSize: Int = 4
}

object IndexSet {
  
  def apply() = new IndexSet
  
  //def apply(length: Int) = new IndexSet(length)
  
}