package latis.util

import org.junit._
import Assert._

class TestMappingIterator {

  @Test 
  def test {
    val it: Iterator[Int] = List(1, 2, 3, 4).iterator.filter(_ != 3)
    val f: Int => Option[Double] = (i: Int) => Some(Math.sqrt(i))
    val it2 = new MappingIterator(it, f) 
    //latis.util.MappingIterator[Int,Any]
    //TODO: why Any?
    assertEquals(2.0, it2.toList(2).asInstanceOf[Double], 0.0)
  }
}