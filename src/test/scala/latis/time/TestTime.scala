package latis.time

import org.junit._
import Assert._
import latis.metadata.Metadata
import latis.dm.Real
import latis.dm.Text

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

  
  @Test
  def julian_date {
    val t = Time(TimeScale.JULIAN_DATE, 2456734.5)
    assertEquals("2014-03-18T00:00:00.000", t.format(TimeFormat.ISO))
  }
  
  
  //construction without data, make sure metadata captures defaults
  
  @Test def text_type_with_units = {
    val t = Time("text", Metadata(Map("units" -> "yyyy-MM-dd")))
    assertTrue(t.isInstanceOf[Text])
    assertEquals("10", t.getMetadata("length").get)
    assertEquals("milliseconds since 1970-01-01", t.getUnits.toString) //default numeric units
  }
  
  @Test def text_type_without_units = {
    val t = Time("text", Metadata.empty)
    assertTrue(t.isInstanceOf[Text])
    assertEquals("23", t.getMetadata("length").get)
    assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSS", t.getMetadata("units").get)
    assertEquals("milliseconds since 1970-01-01", t.getUnits.toString) //default numeric units
  }

  //@Test def text_type_with_units_and_length = ???
  //@Test def text_type_with_units_and_length_too_long = ???
  //@Test def text_type_with_units_and_length_too_short = ???
  //@Test def text_type_with_invalid_units = ???
  
  @Test def real_type_with_units = {
    val t = Time("real", Metadata(Map("units" -> "years since 2000-01-01")))
    assertTrue(t.isInstanceOf[Real])
    assertEquals("years since 2000-01-01", t.getUnits.toString)
  }
  
  @Test def real_type_without_units = {
    val t = Time("real", Metadata.empty)
    assertTrue(t.isInstanceOf[Real])
    assertEquals("milliseconds since 1970-01-01", t.getUnits.toString)
  }
  //@Test def real_type_with_invalid_units = ???
  //@Test def int_type_with_units = ???
  //@Test def int_type_without_units = ???
  //@Test def int_type_with_invalid_units = ???
  
}