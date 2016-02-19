package latis.ops

import org.junit._
import org.junit.Assert._
import latis.dm._
import latis.time._
import latis.metadata.Metadata

class TestTimeTupleToTime {
  
  @Test
  def date_only = {
    val year = Text(Metadata("units" -> "yyyy"), "2015")
    val mon = Text(Metadata("units" -> "MM"), "11")
    val day = Text(Metadata("units" -> "dd"), "01")
    val ds = Dataset(Tuple(List(year, mon, day), Metadata("time"))) //Tuple must have name="time"
    val op = TimeTupleToTime()
    val ds2 = op(ds)
    ds2 match {
      case Dataset(t: Time) => assertEquals(1446336000000l, t.getJavaTime)
    }
  }
  
  @Test
  def date_and_time = {
    val year = Text(Metadata("units" -> "yyyy"), "1970")
    val mon  = Text(Metadata("units" -> "MM"), "1")
    val day  = Text(Metadata("units" -> "dd"), "01")
    val hour = Text(Metadata("units" -> "HH"), "0")
    val min  = Text(Metadata("units" -> "mm"), "0")
    val sec  = Text(Metadata("units" -> "ss"), "01")
    val ds = Dataset(Tuple(List(year, mon, day, hour, min, sec), Metadata("time")))
    val op = TimeTupleToTime()
    val ds2 = op(ds)
    ds2 match {
      case Dataset(t: Time) => assertEquals(1000l, t.getJavaTime)
    }
  }
  
  @Test
  def in_function_domain = {
    val year = Text(Metadata("units" -> "yyyy"), "2015")
    val mon  = Text(Metadata("units" -> "MM"), "11")
    val day  = Text(Metadata("units" -> "dd"), "01")
    val time = Tuple(List(year, mon, day), Metadata("time"))
    val ds = Dataset(Function(List(Sample(time, Real(3.14)))))
    val op = TimeTupleToTime()
    val ds2 = op(ds)
    ds2 match {
      case Dataset(Function(it)) => it.next match {
        case Sample(t: Time, _) => assertEquals(1446336000000l, t.getJavaTime)
      }
    }
  }
}