package latis.util

import org.junit._
import Assert._

class TestMappingIterator {

  //@Test 
  def test {
    val it = List(1, 2, 3, 4).iterator.filter(_ != 3)
    val f = (i: Int, index: Int) => Some(Math.sqrt(i))
    val it2 = new IndexedIterator(it, f)

    //it2.toList.map(println(_))
//    for (i <- it2) {
//      println(it2.getIndex + ": " + i)
//    }
  }
}