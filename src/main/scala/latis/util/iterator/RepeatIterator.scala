package latis.util.iterator

/**
 * Returns each value in the Iterator rep times when getNext is called before moving to the next value. 
 */
class RepeatIterator[T >: Null](it: Iterator[T], rep: Int) extends PeekIterator[T] {
  var count = rep
  def getNext = {
    count += 1
    if(count < rep) current
    else {count = 0; if(it.hasNext) it.next else null }
  }
}
