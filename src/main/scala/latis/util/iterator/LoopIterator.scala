package latis.util.iterator

/**
 * Loops an Iterator such that it returns to its beginning when its end is reached. 
 * Should be zipped with a non-looped Iterator in order to prevent an infinite loop.
 */
class LoopIterator[T >: Null](iterator: Iterator[T]) extends PeekIterator[T] {
  private var it = iterator.duplicate
  def getNext: T = {
    if(it._1.hasNext) it._1.next
    else {
      it = it._2.duplicate
      getNext
    }
  }
}
