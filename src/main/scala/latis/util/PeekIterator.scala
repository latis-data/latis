package latis.util

/**
 * An Iterator that looks ahead and caches the next sample.
 * This makes it easier to know when we are done, especially 
 * when we are filtering. The original source may have another 
 * sample but this will keep looking for the next valid sample.
 * You can also "peek" at the next sample without advancing.
 */
abstract class PeekIterator[T] extends Iterator[T] {
  //TODO: construct by wrapping another Iterator with a predicate: T=>Boolean
  
  /**
   * Cached next value. Will be null if there is no more elements.
   */
  private var _next: T = _ //Note, can't use null due to type checker
  
  /**
   * Use this lazy val so initialization will happen when this is first asked
   * if it is initialized.
   */
  private lazy val _initialized: Boolean = {
    _next = getNext
    true
  }
  
  /**
   * True if there is a cached next value.
   * Note, the first call to this will cause the first value to be accessed and cached.
   */
  final def hasNext: Boolean = _initialized && _next != null
  
  /**
   * Return the next value and cache the next next value
   * to effectively advance to the next sample.
   */
  final def next: T = {
    //TODO: assume hasNext has been called so we don't have to check if this has been initialized?
    //  not sure if this works for it.map
    _initialized
    val current = _next
    _next = getNext //TODO: get next value to cache asynchronously?
    current
  }
  
  /**
   * Take a look at the next sample without advancing to it.
   */
  final def peek: T = _next
  
  /**
   * Override to apply internal logic for getting the next sample.
   * Return null if there are no more elements.
   */
  protected def getNext: T 
  //TODO: use option?
  
}

