package latis.util

/**
 * Utility class that produces the first value once
 * then the other on subsequent calls.
 * Handy for delimiters.
 */
class FirstThenOther[T](first: T, other: T) {

  private var _hasBeenCalled = false
  
  def value: T = {
    if (! _hasBeenCalled) {
      _hasBeenCalled = true
      first
    } else other
  }
}

object FirstThenOther {
  def apply[T](first: T, other: T): FirstThenOther[T] = new FirstThenOther[T](first, other)
}
