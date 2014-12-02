package latis.util

import org.junit._
import Assert._

class TestIterators {

  @Test 
  def map_sqrt {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator.filter(_ != 3)
    val f: Int => Option[Double] = (i: Int) => Some(Math.sqrt(i))
    val it2 = new MappingIterator(it, f) 
    //latis.util.MappingIterator[Int,Any]
    //TODO: why Any?
    assertEquals(2.0, it2.toList(2).asInstanceOf[Double], 0.0)
  }
  
  @Test
  def peek_first {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val a = pit.peek
    assertEquals(1, a)
  }
  
  @Test
  def peek_second {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val b = pit.next
    val a = pit.peek
    assertEquals(2, a)
  }
  
  @Test
  def peek_after_last {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val b = pit.toList
    val a = pit.peek
    assertEquals(null, a)
  }
  
  //@Test
  def dont_double_wrap {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val pit2 = PeekIterator(pit)
    //TODO: test that pit and pit2 are the same object
  }
}