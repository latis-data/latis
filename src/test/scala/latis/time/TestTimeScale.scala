package latis.time

import org.junit._
import Assert._
import java.util.Date

class TestTimeScale {

  @Test
  def can_change_default_time_scale_type = {
    //maybe not a good thing, but needed for tests
    System.setProperty("time.scale.type", "UTC")
    val ts1 = TimeScale.JAVA
    assertEquals("UTC", ts1.tsType.toString)
    
    System.clearProperty("time.scale.type")
    val ts2 = TimeScale.JAVA
    assertEquals("NATIVE", ts2.tsType.toString)
  }
  
  @Test
  def construct_with_date = {
    val ts = TimeScale(new Date(0), TimeUnit.MILLISECOND, TimeScaleType.UTC)
    val s = ts.toString
    assertEquals("UTC milliseconds since 1970-01-01", s)
  }
  
  @Test
  def construct_with_date_string = {
    val ts = TimeScale("1980-01-06", TimeUnit.MICROSECOND, TimeScaleType.TAI)
    val s = ts.toString
    assertEquals("TAI microseconds since 1980-01-06", s)
  }
  
  @Test
  def change_time_scale_type = {
    System.clearProperty("time.scale.type")
    val ts1 = TimeScale.JAVA
    val ts2 = TimeScale(ts1, TimeScaleType.UTC)
    assertEquals("milliseconds since 1970-01-01", ts1.toString) //immutable
    assertEquals("UTC milliseconds since 1970-01-01", ts2.toString)
  }
  
  @Test
  def construct_from_numeric_unit_string = {
    val s = "milliseconds since 1970-01-01"
    val ts = TimeScale(s)
    assertEquals("milliseconds since 1970-01-01", ts.toString)
  }
  
  @Test
  def construct_from_numeric_unit_string_with_type = {
    val s = "tai milliSeconds since 1970-01-01"
    val ts = TimeScale(s)
    assertEquals("TAI milliseconds since 1970-01-01", ts.toString)
    assertEquals("milliseconds", ts.unit.toString)
    assertEquals(new java.util.Date(0), ts.epoch)
    assertEquals("TAI", ts.tsType.toString)
  }
  
  @Test
  def construct_from_numeric_unit_string_with_time_scale_type_from_property = {
    //numeric units shouldn't use time.scale.type
    System.setProperty("time.scale.type", "UTC")
    val s = "milliseconds since 1970-01-01"
    val ts = TimeScale(s)
    assertEquals("milliseconds since 1970-01-01", ts.toString)
  }
  
  @Test
  def construct_from_formatted_unit_string = {
    System.setProperty("time.scale.type", "UTC")
    val s = "yyyy-MM-dd"
    val ts = TimeScale(s)
    assertEquals("milliseconds", ts.unit.toString) //TimeScale from formatted units will use Java time
    assertEquals(new java.util.Date(0), ts.epoch)
    assertEquals("UTC", ts.tsType.toString) //TimeScale from formatted units will use time.scale.type
    assertEquals("yyyy-MM-dd", ts.toString) //string representation will preserve the format
  }
  
  
  //TODO: test invalid inputs
}