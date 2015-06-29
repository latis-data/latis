package latis.time

import java.util.Date

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

import latis.time.TimeScaleType.TAI
import latis.time.TimeScaleType.UTC
import latis.time.TimeUnit.SECOND

class TestTaiToUtcConverter {
  
  @Test
  def test_construction {
    val ts = TimeScale.JAVA
    val utc = TimeScale(ts, UTC)
    val tai = TimeScale(ts, TAI)
    val conv = TimeConverter(tai, utc)
    assertTrue(conv.isInstanceOf[TaiToUtcConverter])
  }
    
  @Test
  def test_before_leap_second {
    val epoch = new Date(TimeFormat.DATE.parse("2008-12-31")) //date with a leap second
    val tai = TimeScale(epoch, SECOND, TAI)
    val t1 = Time(tai, 86399)
    val utc = TimeScale(epoch, SECOND, UTC)
    val time = t1.convert(utc).getNumberData.doubleValue
    assertEquals(86399, time, 0)
  }

  @Test
  def test_start_of_leap_second() {
    val epoch = new Date(TimeFormat.DATE.parse("2008-12-31")) //date with a leap second
    val tai = TimeScale(epoch, SECOND, TAI)
    val t1 = Time(tai, 86400)
    val utc =  TimeScale(epoch, SECOND, UTC)
    val time = t1.convert(utc).getNumberData.doubleValue
    assertEquals(86400, time, 0)
  }

  @Test
  def test_during_leap_second {
    val epoch = new Date(TimeFormat.DATE.parse("2008-12-31")) //date with a leap second
    val tai = TimeScale(epoch, SECOND, TAI)
    val t1 = Time(tai, 86400.5)
    val utc =  TimeScale(epoch, SECOND, UTC)
    val time = t1.convert(utc).getNumberData.doubleValue
    assertEquals(86400, time, 0)
  }

  @Test
  def test_second_after_leap_second {
    val epoch = new Date(TimeFormat.DATE.parse("2008-12-31")) //date with a leap second
    val tai = TimeScale(epoch, SECOND, TAI)
    val t1 = Time(tai, 86401)
    val utc =  TimeScale(epoch, SECOND, UTC)
    val time = t1.convert(utc).getNumberData.doubleValue
    assertEquals(86400, time, 0)   
  }
    
  @Test
  def test_two_seconds_after_leap_second {
    val epoch = new Date(TimeFormat.DATE.parse("2008-12-31")) //date with a leap second
    val tai = TimeScale(epoch, SECOND, TAI)
    val t1 = Time(tai, 86402)
    val utc =  TimeScale(epoch, SECOND, UTC)
    val time = t1.convert(utc).getNumberData.doubleValue
    assertEquals(86401, time, 0)
  }

  @Test
  def test_before_tai_epoch {
    //TAI epoch is 1958, first leap second in 1972
    val epoch = new Date(TimeFormat.DATE.parse("1900-01-01"))
    val utc = TimeScale(epoch, SECOND, UTC)
    val tai = TimeScale(epoch, SECOND, TAI)
    val one_year = TimeUnit.YEAR.seconds
    val t1 = Time(tai, one_year)
    val t2 = t1.convert(utc).getNumberData.doubleValue
    assertEquals(one_year, t2, 0)
  }
    
  @Test
  def test_to_earlier_epoch_with_leap_second_between {
    // -3  -2  -1  TAI
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // UTC  1   1   2

    val ts1 = TimeScale("2009-01-01T00:00:01.000", SECOND, TAI)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 2
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_earlier_epoch_at_leap_second_end {
    // -2  -1  TAI
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // UTC  1   1   2

    val ts1 = TimeScale("2009-01-01", SECOND, TAI)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 1
        
    assertEquals(expected, t2, 0)
  }
   
  @Test
  def test_to_earlier_epoch_at_leap_second_start {
    // -2  -1  TAI
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // UTC  1   1   2

    val ts1 = TimeScale("2009-01-01", SECOND, TAI)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val t1 = Time(ts1, -1)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 1
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_earlier_epoch_within_leap_second {
    // -2  -1  TAI
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // UTC  1   1   2

    val ts1 = TimeScale("2009-01-01", SECOND, TAI)
    val ts2 = TimeScale("2008-12-31T23:59:59.000", SECOND, UTC)
    val t1 = Time(ts1, -0.5)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 1
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_later_epoch_with_leap_second_between() {
    // TAI  1   2   3
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // -2  -1  -1  UTC

    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val ts2 = TimeScale("2009-01-01T00:00:01.000", SECOND, UTC)
    val t1 = Time(ts1, 3)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_later_epoch_at_leap_second_end {
    // TAI  1   2   3
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // -1   0  UTC

    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val ts2 = TimeScale("2009-01-01", SECOND, UTC)
    val t1 = Time(ts1, 2)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_later_epoch_at_leap_second_start {
    // TAI  1   2   3
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // -1   0  UTC

    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val ts2 = TimeScale("2009-01-01", SECOND, UTC)
    val t1 = Time(ts1, 1)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }
    
  @Test
  def test_to_later_epoch_within_leap_second() {
    // TAI  1   2   3
    //--+---[ls)+---+-->
    //--+---+...+---+-->
    // -1   0  UTC

    val ts1 = TimeScale("2008-12-31T23:59:59.000", SECOND, TAI)
    val ts2 = TimeScale("2009-01-01", SECOND, UTC)
    val t1 = Time(ts1, 1.5)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    val expected = 0
    
    assertEquals(expected, t2, 0)
  }


}