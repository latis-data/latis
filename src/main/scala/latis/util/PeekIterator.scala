package latis.util

/**
 * An Iterator that looks ahead and caches the next sample.
 * This makes it easier to know when we are done, especially 
 * when we are filtering. The original source may have another 
 * sample but this will keep looking for the next valid sample.
 * You can also "peek" at the next sample without advancing.
 */
abstract class PeekIterator[T] extends Iterator[T] {
  
//  /**
//   * Override to initialize stuff before we try to get the first sample.
//   */
//  def init = {} 
//  
//  /**
//   * Private initialization so we can get the first sample staged
//   * but after allowing subclasses to initialize.
//   */
//  private def _init = {
//    init
//    _next = getNext()
//  }
//  
//  _init
  
  //private var _initialized = false
  
  /**
   * Cached next value. Will be null if there is no more elements.
   */
  private var _next: T = _ //getNext() //Prime this Iterator with the first sample.
  //TODO: delay till iteration starts
  //  can't use lazy for var
  //  subclass impl needs to init stuff before this is invoked
  //  set to something other than null - unitilialized?
  //  use Option for _next, null for un-init?
  //  avoid adding extra logic to each iteration
  //  but subclass getNext will likely have plenty of logic, pattern matching,...
  
  private lazy val _initialized: Boolean = {
    _next = getNext
    true
  }
  
  /**
   * True if there is a cached next value.
   */
  final def hasNext: Boolean = {
//    //Initialize (stage the first sample) 
//    if (! _initialized) {
//      _next = getNext
//      _initialized = true
//    }
    _initialized && _next != null
  }
  
  /**
   * Return the next value and cache the next next value
   * to effectively advance to the next sample.
   */
  final def next(): T = {
    //TODO: assume hasNext has been called so we don't have to check if this has been initialized?
    //  not sure if this works for it.map
    _initialized
    val current = _next
    _next = getNext() //TODO: async
    current
  }
  
  /**
   * Take a look at the next sample without advancing to it.
   */
  final def peek(): T = _next
  
  /**
   * Override to apply internal logic for getting the next sample.
   */
  protected def getNext(): T 
  
}

