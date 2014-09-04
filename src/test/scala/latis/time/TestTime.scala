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

  @Test
  def iso_with_millis_to_millis {
    val ms = Time.isoToJava("1970-01-01T00:00:00.001")
    assertEquals(1, ms)
  }

  @Test
  def iso_with_millis_to_millis_withZ {
    val ms = Time.isoToJava("1970-01-01T00:00:00.001Z")
    assertEquals(1, ms)
  }
  
  @Test
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
  
 @Test
  def iso_ordinal_date {
    val ms = Time.isoToJava("1970-001")
    assertEquals(0, ms)
  }
 
 @Test
  def iso_ordinal_date_not_month {
    //javax.xml.bind.DatatypeConverter interprets day of year as month
    val ms = Time.isoToJava("1970-002")
    assertEquals(86400000, ms)
  }
  
  @Test
  def iso_ordinal_date_with_time {
    val ms = Time.isoToJava("1970-001T00:00:01")
    assertEquals(1000, ms)
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
  
  //@Test 
  def time_as_tuple {
//timed_see_ssi_l3a
//    <time units="yyyyDDD ???"/>
//      <text name="DATE" />
//      <real name="TIME" units="seconds"/>
//    </time>
    //TODO: start with date+time only?
    //as many text elements as needed: yyyy, mm, dd, hh...
    //append text vars in order (comma delim?)
    //always convert to java ms? or only if there is a numeric component?
    //add numeric component, converted to ms
    /*
     * can we use the tuple representation in the model (1st pass) then convert to scalar as we parse data?
     * how attached are we to the idea of Time being a scalar?
     *   domain tuple arity is a handy indicator of number of dimensions
     *   (date,time) would be a 1D manifold in this case?
     *   some data tables are 2D (year,month)
     * derived field?
     *   date and time could be in range, groupBy time after
     *   just a sum
     * consider how columnar adapter can stitch diff vars together
     */
    val d = Text(Metadata("Date"), "2014-01-01")
    val t = Real(Metadata(Map("name" -> "Time", "units" -> "seconds")), 123.0)
    //val time = Time(List(d,t), Metadata(Map("units" -> "seconds")))
    
    
  }
  
}