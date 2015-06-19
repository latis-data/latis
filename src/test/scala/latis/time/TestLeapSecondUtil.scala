package latis.time

import java.util.Date
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.Ignore

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

  @Test @Ignore //won't work until July when upcoming ls hits
  def test_future {
    val now = new Date()
    val time = now.getTime() + 1000000000
    val date = new Date(time)
    val lsnow = LeapSecondUtil.getLeapSeconds(now)
    val ls = LeapSecondUtil.getLeapSeconds(date)
    assertEquals(ls, lsnow, 0)
  }

}