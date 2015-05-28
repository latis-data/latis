package latis.time

import java.util.Date

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

import latis.time.TimeScaleType.TAI
import latis.time.TimeUnit.MINUTE
import latis.time.TimeUnit.SECOND

class TestTaiToTaiConverter {
  
  @Test
  def test_construction_same_epoch() {
    //Don't need a special converter if the epochs are the same.
    val e1 = new Date(Time.isoToJava("2009-01-01"))
    val ts1 = TimeScale(e1, SECOND, TAI)
    val ts2 = TimeScale(e1, MINUTE, TAI)
    val conv = TimeConverter(ts1, ts2)
    assertTrue(! (conv.isInstanceOf[TaiToTaiConverter]))
  }

  @Test
  def test_construction_diff_epoch() {
    val e1 = new Date(Time.isoToJava("2009-01-01"))
    val e2 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val ts1 = TimeScale(e1, SECOND, TAI)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val conv = TimeConverter(ts1, ts2)
    assertTrue(conv.isInstanceOf[TaiToTaiConverter])
  }

  @Test
  def test_diff_epoch_over_leap_second() {
    // -3  -2  -1  TAI
    //--+---[ls)+---+-->
    //--+---[ls)+---+-->
    // TAI  1   2   3
    val e1 = new Date(Time.isoToJava("2009-01-01T00:00:01.000"))
    val e2 = new Date(Time.isoToJava("2008-12-31T23:59:59.000"))
    val ts1 = TimeScale(e1, SECOND, TAI)
    val ts2 = TimeScale(e2, SECOND, TAI)
    val t1 = Time(ts1, 0)
    val t2 = t1.convert(ts2).getNumberData.doubleValue
    assertEquals(3, t2, 0)
  }

}