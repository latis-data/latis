package latis.util

/**
 * An Iterator that looks ahead and caches the next sample.
 * This makes it easier to know when we are done, especially 
 * when we are filtering. The original source may have another 
 * sample but this will keep looking for the next valid sample.
 * You can also "peek" at the next sample without advancing.
 */
class IndexedIterator[S,T >: Null](iterator: Iterator[S], f: (S,Int) => Option[T]) extends PeekIterator[T] {
  //Note, the bound on Null allows us to return null for generic type T.
  
  /**
   * Manage the current index.
   */
  private var _index = -1
  
  /**
   * Return the current index.
   */
  final def getIndex = _index
  
  
  /**
   * Responsible for getting the next transformed item 
   * or null if there are no more valid items.
   * This will keep trying until a valid sample is found 
   * or it hits the end of the original iterator.
   */
  protected def getNext: T = {
    if (iterator.hasNext) {
      //apply the operation
      f(iterator.next, getIndex) match {
        case None => getNext //invalid value, try another
        case Some(t) => {
          _index += 1 //increment the current index
          t
        }
      }
    } else null
  }
}
