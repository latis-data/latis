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
    val list2 = list.sorted(Integer(0)) //note, we are passing the Integer as an explicit Ordering (LATIS-532)
    val list3 = list2.map(i => i match {case Integer(n) => n})
    assertEquals(1, list3(0))
    assertEquals(2, list3(1))
    assertEquals(3, list3(2))
  }  
  
  @Test
  def real = {
    val list: List[Real] = List(Real(2.2), Real(3.3), Real(1.1))
    val list2 = list.sorted(Real(0)) 
    val list3 = list2.map(i => i match {case Real(n) => n})
    assertEquals(1.1, list3(0), 0.0)
    assertEquals(2.2, list3(1), 0.0)
    assertEquals(3.3, list3(2), 0.0)
  }
    
  @Test
  def text = {
    val list: List[Text] = List(Text("b"), Text("c"), Text("a"))
    val list2 = list.sorted(Text("")) 
    val list3 = list2.map(i => i match {case Text(n) => n})
    assertEquals("a", list3(0))
    assertEquals("b", list3(1))
    assertEquals("c", list3(2))
  }
    
  @Test
  def time_as_text = {
    val list = List(Text("2015-02-01T15:00:00"), Text("2015-02-01T15:00:01"), Text("2015-01-01"))
    val list2 = list.sorted(Text("")) 
    val list3 = list2.map(i => i match {case Text(n) => n})
    assertEquals("2015-01-01", list3(0))
    assertEquals("2015-02-01T15:00:00", list3(1))
    assertEquals("2015-02-01T15:00:01", list3(2))
  }
}