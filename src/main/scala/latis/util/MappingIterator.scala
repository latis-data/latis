package latis.util

import scala.annotation.tailrec

/**
 * An Iterator that looks ahead and caches the next sample.
 * This makes it easier to know when we are done, especially 
 * when we are filtering. The original source may have another 
 * sample but this will keep looking for the next valid sample.
 * You can also "peek" at the next sample without advancing.
 * This implementation of the PeekIterator applies a function
 * mapping to each element as it iterates.
 */
class MappingIterator[S,T >: Null](iterator: Iterator[S], f: S => Option[T]) extends PeekIterator[T] {
  //Note, the bound on Null allows us to return null for generic type T.
  //TODO: could we do it.flatMap(f)? but would no longer be a PeekIterator? unless we do CanBuildFrom...?

  /**
   * Responsible for getting the next transformed item 
   * or null if there are no more valid items.
   * This will keep trying until a valid sample is found 
   * or it hits the end of the original iterator.
   */
  @tailrec final protected def getNext: T = {
    if (! iterator.hasNext) null
    else {
      //Apply the operation. Skip element if there is an Exception.
      val result = try {
        f(iterator.next)
      } catch {
        case e: Exception => None //TODO: log warning?
      }
      
      result match {
        case Some(t) => t
        case None => getNext //invalid value, try another
      }
    }
  }
}
