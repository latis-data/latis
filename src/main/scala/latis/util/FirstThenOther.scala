package latis.util

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
  def apply[T](first: T, other: T) = new FirstThenOther[T](first, other)
}