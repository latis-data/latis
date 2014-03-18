package latis.time

import org.junit._
import Assert._

class TestTime {

  @Test
  def iso_to_millis {
    val ms = Time.isoToJava("1970-01-01T00:00:00")
    assertEquals(0, ms)
  }
  
  //@Test
  //TODO: not supported by javax.xml.bind.DatatypeConverter.parseDateTime
  def iso_without_T_to_millis {
    val ms = Time.isoToJava("1970-01-01 00:00:00")
    assertEquals(0, ms)
  }
  
  @Test
  def iso_date_to_millis {
    val ms = Time.isoToJava("1970-01-01")
    assertEquals(0, ms)
  }
  
  //TODO: test other flavors, with time zone,...

  
  //@Test
  def julian_date {
    val t = Time(TimeScale.JULIAN_DATE, 2456734.5)
    println(t.format(TimeFormat.ISO))  //-4713-11-24T12:40:56.735  but http://aa.usno.navy.mil/data/docs/JulianDate.php says 2014-03-18
  }
}