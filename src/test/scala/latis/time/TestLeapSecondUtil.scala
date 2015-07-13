package latis.time

import java.util.Date
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.Ignore
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestLeapSecondUtil {
  
  @Test
  def test_at_leap_second() {
    val date: Date = new Date(TimeFormat.DATE.parse("2009-01-01"))
    val d: Double = LeapSecondUtil.getLeapSeconds(date)
    assertEquals(34, d, 0)
  }

  @Test
  def test_before_leap_second {
    val date = new Date(TimeFormat.DATETIME.parse("2008-12-31T23:59:59.999"))
    val d = LeapSecondUtil.getLeapSeconds(date)
    assertEquals(33, d, 0)
  }

  @Test
  def test_before_leap_seconds_existed {
    val date = new Date(TimeFormat.DATE.parse("1950-01-01"))
    val d = LeapSecondUtil.getLeapSeconds(date)
    assertEquals(0, d, 0)
  }
  
  @Test @Ignore //bug with negative times (LATIS-334)
  def utc_to_future_tai_to_utc_at_java_epoch = {
    val utcts = TimeScale("1970-01-01", TimeUnit.MILLISECOND, TimeScaleType.UTC)
    val taits = TimeScale("1980-01-06", TimeUnit.MICROSECOND, TimeScaleType.TAI)
    val utc = Time(utcts, 0)
    //val utc = Time(utcts, 315964800000l) //this works
    val tai = utc.convert(taits)
    val utc2 = tai.convert(utcts)
    val s = utc2.format("yyyy-MM-dd HH:mm:ss").toString
    /*
     * 10 years, 5 days
     * 2 leap days
     * 19 leap seconds
     * ((10 * 365) + 7) * 86400 + 19 = 315964819s
     * we get 315964800s
     * problem with negative values
     * leap seconds added for epoch offset then removed for the rest
     */
    assertEquals(315964819000000l, tai.getNumberData.longValue) //IDL datetime reports -315964812814318
    assertEquals("1970-01-01 00:00:00", s)
  }
  
  @Test
  def utc_to_past_tai_to_utc_at_java_epoch = {
    val utcts = TimeScale("1970-01-01", TimeUnit.MILLISECOND, TimeScaleType.UTC)
    val taits = TimeScale("1958-01-01", TimeUnit.SECOND, TimeScaleType.TAI)
    val utc = Time(utcts, 0)
    val tai = utc.convert(taits)
    val utc2 = tai.convert(utcts)
    val s = utc2.format("yyyy-MM-dd HH:mm:ss").toString
    /*
     * 12 years
     * 3 leap days
     * 0 leap seconds
     * ((12 * 365) + 3) * 86400 = 378691200
     */
    assertEquals(378691200, tai.getNumberData.longValue)
    assertEquals("1970-01-01 00:00:00", s)
  }
  
  @Test
  def test_future {
    //We shouldn't know any more than the next leap second (usually 6 months out)
    val now = new Date()
    val next_year = new Date(now.getTime() + 31536000000l)
    val lsnow = LeapSecondUtil.getLeapSeconds(now)
    val ls = LeapSecondUtil.getLeapSeconds(next_year)
    val dls = ls - lsnow
    assert(dls == 0 || dls == 1)
  }

}
