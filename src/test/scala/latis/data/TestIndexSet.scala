package latis.data

import org.junit._
import Assert._
import latis.data.set.IndexSet

class TestIndexSet {

  @Test
  def iterate_twice {
    val iset = IndexSet()
    val v1 = iset.iterator.take(1).toList(0)
    val v2 = iset.iterator.take(1).toList(0)
    assertEquals(v1,v2)
  }
}