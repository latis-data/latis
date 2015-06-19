package latis.time

import java.util.Date

import org.junit.Assert.assertEquals
import org.junit.Test

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
    assertEquals(10, d, 0)
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