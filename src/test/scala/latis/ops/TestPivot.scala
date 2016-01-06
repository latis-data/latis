package latis.ops

import org.junit.Test
import org.junit.Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.dm._

class TestPivot {
  
  @Test
  def basic_pivot {
    val ds = TsmlReader("pivot.tsml").getDataset
    val data = ds.toDoubles
    assertEquals(-999, data(1)(0), 0)
    assertEquals(5, data(3)(2), 0)
  }
  
  @Test
  def names {
    val ds = TsmlReader("pivot.tsml").getDataset
    ds match {
      case Dataset(f: Function) => f.getRange match {
        case Tuple(Seq(f1,f2,f3)) 
          if(f1.getName == "foo1" &&
             f2.getName == "foo2" &&
             f3.getName == "foo3") => 
        case _ => fail
      }
    }
  }
  
}