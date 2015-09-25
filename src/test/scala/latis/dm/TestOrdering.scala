package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.LazyLogging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter
import latis.writer.Writer
import latis.data.Data
import latis.data.SampledData
import latis.data.seq.DataSeq

class TestOdering {
  
  @Test
  def integer = {
    val list: List[Integer] = List(Integer(2), Integer(3), Integer(1))
    //val lsit2 = list.sorted 
    /*
     * diverging implicit expansion for type scala.math.Ordering[latis.dm.Integer] 
     * starting with method comparatorToOrdering in trait LowPriorityOrderingImplicits
     */
    val list2 = list.sorted(Integer(0)) 
    val list3 = list2.map(i => i match {case Integer(n) => n})
    assertEquals(1, list3(0))
    assertEquals(2, list3(1))
    assertEquals(3, list3(2))
  }
}