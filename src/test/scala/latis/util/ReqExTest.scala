package latis.util

import org.junit._
import Assert._

class RegExTest {
  //TODO: load special properties file, controlled environment
  
  @Test
  def match_time_selection {
    val r = RegEx.TIME findFirstIn "2011-09-14"
    println(r.get)
  }
}