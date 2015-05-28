package latis.time

import java.util.Date

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

import latis.time.TimeScaleType.TAI
import latis.time.TimeScaleType.UTC
import latis.time.TimeUnit.MILLISECOND
import latis.time.TimeUnit.MINUTE
import latis.time.TimeUnit.SECOND
import latis.time.TimeUnit.YEAR

class TestUtcToTaiConverter {
  
  @Test
  def test_construction {
    val ts = TimeScale.DEFAULT
    val utc = TimeScale(ts, UTC)
    val tai = TimeScale(ts, TAI)
    val conv = TimeConverter(utc, tai)
    assertTrue(conv.isInstanceOf[UtcToTaiConverter])
  }
    
  @Test
  def test_before_leap_second {
    val epoch = new Date(Time.isoToJava("2008-12-31")) //day with a leap second
    val utc = TimeScale(epoch, SECOND, UTC)
    val t1 = Time(utc, 86399)
    val tai = TimeScale(epoch, SECOND, TAI)
    val time = t1.convert(tai).getNumberData.doubleValue
    assertEquals(86399, time, 0)
  }

  @Test
 def test_after_leap_second {
    val epoch = new Date(Time.isoToJava("2008-12-31")) //day with a leap second
    val utc = TimeScale(epoch, SECOND, UTC)
    val t1 = Time(utc, 86400)
    val tai = TimeScale(epoch, SECOND, TAI)
    val time = t1.convert(tai).getNumberData.doubleValue
    assertEquals(86401, time, 0)
  }

  @Test
  def test_before_tai_epoch {
    //TAI epoch is 1958, first leap "second" in 1961
    val epoch = new Date(Time.isoToJava("1900-01-01"))
    val utc = TimeScale(epoch, SECOND, UTC)
    val tai = TimeScale(epoch, SECOND, TAI)
    val one_year = YEAR.seconds
    val t1 = Time(utc, one_year)
    val t2 = t1.convert(tai).getNumberData.doubleValue
    assertEquals(one_year, t2, 0)
  }
    
    //-- Tests converting between different epochs that span a leap second.
    
  @Test
  def test_diff_epoch_before_leap_second {
    val e1 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val e2 = new Date(Time.isoToJava("2009-01-01"))
    val ts1 = TimeScale(e1, SECOND, UTC)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val t1 = Time(ts1, 0) //start of last UTC second before leap second
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(-2, t2, 0)
  }

  @Test
  def test_diff_epoch_at_leap_second {
    val e1 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val e2 = new Date(Time.isoToJava("2009-01-01"))
    val ts1 = TimeScale(e1, SECOND, UTC)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val t1 = Time(ts1, 1)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(0, t2, 0)
  }
    
  @Test
  def test_diff_epoch_after_leap_second {
    val e1 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val e2 = new Date(Time.isoToJava("2009-01-01"))
    val ts1 = TimeScale(e1, SECOND, UTC)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val t1 = Time(ts1, 2)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(1, t2, 0)
  }    

  @Test
  def test_diff_epoch_before_leap_second_reversed {
    val e1 = new Date(Time.isoToJava("2009-01-01"))
    val e2 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val ts1 = TimeScale(e1, SECOND, UTC)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val t1 = Time(ts1, -1) //start of last UTC second before leap second
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(0, t2, 0)
  }

  @Test
  def test_diff_epoch_at_leap_second_reversed {
    val e1 = new Date(Time.isoToJava("2009-01-01"))
    val e2 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val ts1 = TimeScale(e1, SECOND, UTC)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(2, t2, 0)
  }

  @Test
  def test_diff_epoch_after_leap_second_reversed {
    val e1 = new Date(Time.isoToJava("2009-01-01"))
    val e2 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val ts1 = TimeScale(e1, SECOND, UTC)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val t1 = Time(ts1, 1)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(3, t2, 0)
  }    
    
    //-- Test converting between scales with different units.
    
  @Test
  def test_diff_units {
    val e1 = new Date(Time.isoToJava("2009-01-01"))
    val e2 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val ts1 = TimeScale(e1, MILLISECOND, UTC)
    val ts2 = TimeScale(e2, MINUTE, TAI)
    val t1 = Time(ts1, 1000)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(0.05, t2, 0)
  }
    
        
    
  @Test
  def test_utc_to_tai_with_earlier_epoch_with_leap_second_between {
    val ts1 = TimeScale("2009-01-01T00:00:01.000", SECOND, UTC)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 3
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_utc_to_tai_with_earlier_epoch_with_leap_second_between_from_negative_time {
    val ts1 = TimeScale("2009-01-01T00:00:01.000", SECOND, UTC)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val t1 = Time(ts1, -2)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_utc_to_tai_with_later_epoch_with_leap_second_between_to_negative_time {
    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val ts2 = TimeScale("2009-01-01T00:00:01.000", SECOND, TAI)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = -3
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_utc_to_tai_with_later_epoch_with_leap_second_between {
    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val ts2 = TimeScale("2009-01-01T00:00:01.000", SECOND, TAI)
    val t1 = Time(ts1, 2)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_earlier_epoch_at_leap_second {
    // -1   0  UTC
    //--+---+...+---+-->
    //--+---[ls)+---+-->
    // TAI  1   2   3

    val ts1 = TimeScale("2009-01-01", SECOND, UTC)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 2
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_earlier_epoch_before_leap_second {
    // -1   0  UTC
    //--+---+...+---+-->
    //--+---[ls)+---+-->
    // TAI  1   2   3

    val ts1 = TimeScale("2009-01-01", SECOND, UTC)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val t1 = Time(ts1, -1)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_later_epoch_at_leap_second {
    // UTC  1   1   2
    //--+---+...+---+-->
    //--+---[ls)+---+-->
    // -2  -1  TAI

    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val ts2 = TimeScale("2009-01-01", SECOND, TAI)
    val t1 = Time(ts1, 1)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_later_epoch_before_leap_second {
    // UTC  1   1   2
    //--+---+...+---+-->
    //--+---[ls)+---+-->
    // -2  -1  TAI

    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val ts2 = TimeScale("2009-01-01", SECOND, TAI)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = -2
    
    assertEquals(expected, t2, 0)
  }

}