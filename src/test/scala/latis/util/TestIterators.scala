package latis.util

import org.junit._
import Assert._
import latis.util.iterator.MappingIterator
import latis.util.iterator.PeekIterator
import java.io.IOException

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
  
  @Test
  def dont_double_wrap {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val pit2 = PeekIterator(pit)
    assert(pit eq pit2) //same instance
  }
  
  @Test
  def find {
    //Note, 'find' in general invalidates an iterator but seems to work here
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val b = pit.find(_ == 3)
    val a = pit.peek
    assertEquals(4, a)
  }
  
  @Test
  def find_twice {
    //Note, 'find' in general invalidates an iterator but seems to work here
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val b = pit.find(_ == 2)
    val c = pit.find(_ == 3)
    val a = pit.peek
    assertEquals(4, a)
  }
  
  @Test
  def find_twice_same_value {
    //Note, can only find later values
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val b = pit.find(_ == 2)
    val c = pit.find(_ == 2)
    val a = pit.peek
    assertEquals(null, a)
  }
  
  
  @Test
  def current_first {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val a = pit.current
    assertEquals(null, a)
  }
  
  @Test
  def current_last {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val b = pit.find(_ == 4)
    val a = pit.current
    assertEquals(4, a)
  }
  
  @Test
  def current_after_last {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator
    val pit = PeekIterator(it)
    val b = pit.toList
    val a = pit.current
    assertEquals(4, a)
  }
  
  @Test
  def lazy_peek_iterator = { //don't access source until asking. LATIS-403
    var initialized = false
    var hasNextCalled = false
    var counter = 0
    
    //make Iterator representing the original data source
    val it = new Iterator[Int] {
      initialized = true
      def hasNext = {
        hasNextCalled = true
        counter < 10
      }
      def next = {
        counter = counter + 1
        counter
      }
    }
    
    //make PeekIterator from a regular Iterator and make sure it doesn't access data
    //note that if the original iterator accesses data during construction, there is nothing we can do
    val pit = PeekIterator(it)
    assert(initialized)
    assert(!hasNextCalled)
    assertEquals(0, counter) 
    
    //peek at the first element and see that it invoked the Iterator's hasNext and next methods
    pit.peek
    assert(hasNextCalled)
    assertEquals(1, counter) 
  }
  
  @Test(expected=classOf[RuntimeException])
  def abort_mapping_iterator = {
    //Make iterator that throws IOException
    val it = new Iterator[Int] {
      var n = 0
      def hasNext: Boolean = true
      def next: Int = {
        n = n + 1
        if (n > 5) throw new IOException("Test exception.")
        n
      }
    }
    
    val mit = new MappingIterator(it, (x: Int) => Some(x))
    val l = mit.toList //trigger MappingIterator to deal with IOException
  }
}