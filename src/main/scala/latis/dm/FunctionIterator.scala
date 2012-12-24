package latis.dm

/**
 * An Iterator for Functions that returns domain/range Variable pairs.
 * It is designed to cache the next sample and allows you to peek at it.
 */
class FunctionIterator extends Iterator[(Variable, Variable)] {

  /**
   * Internally wrapped Iterator.
   */
  protected var _iterator: Iterator[(Variable, Variable)] = null
  
  /**
   * Cached next value. Will be null if there is no more elements.
   */
  protected var _next: (Variable, Variable) = null
  
  /**
   * True if there is a cached next value.
   */
  def hasNext: Boolean = _next != null
  
  /**
   * Return the next value and cache the next next value
   * to effectively advance to the next sample.
   */
  def next(): (Variable, Variable) = {
    val current = _next
    _next = getNextSample()
    current
  }
  
  /**
   * Take a look at the next sample without advancing to it.
   */
  def peek(): (Variable, Variable) = _next
  
  /**
   * Internal logic for getting the next sample.
   * Useful for subclasses to override.
   */
  protected def getNextSample(): (Variable, Variable) = if (_iterator.hasNext) _iterator.next() else null
  
}

object FunctionIterator {
  
//  def apply(f: Function) = {
//    val it = f.getDataset().getAccessor().getIterator(f)
//    val fit = new FunctionIterator()
//    fit._iterator = it
//    
//  }
  
}
