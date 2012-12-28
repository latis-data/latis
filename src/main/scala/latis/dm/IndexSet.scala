package latis.dm

class IndexSet(mylength: Int) extends DomainSet {
  //Note, we used "mylength" because "length" and "size" are already defined for Iterator.
  
  def iterator = new Iterator[Index]() {
    var index = -1
  
    def hasNext(): Boolean = {
      index + 1 < mylength
    }
  
    def next() = {
      index += 1
      Index(index)
    }
  }
}

object IndexSet {
  
  def apply(length: Int) = new IndexSet(length)
}