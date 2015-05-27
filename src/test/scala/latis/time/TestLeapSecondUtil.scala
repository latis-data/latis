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
    val now = new Date()
    val time = now.getTime() + 1000000000
    val date = new Date(time)
    val lsnow = LeapSecondUtil.getLeapSeconds(now)
    val ls = LeapSecondUtil.getLeapSeconds(date)
    assertEquals(ls, lsnow, 0)
  }

//  @Test
//  public void test_reading_leapseconds_from_data_dir() {
//    String dir = System.getProperty("user.dir") + "/src/lasp/time/convert";
//    System.setProperty("data.dir", dir);
//    BufferedReader reader = LeapSecondUtil.getReaderFromDataDir();
//    String line = "";
//    try {
//        line = reader.readLine();
//    } catch (IOException e) {
//        e.printStackTrace();
//    } finally {
//        try {reader.close();} catch (Exception e) {}
//    }
//    
//    assert(line.startsWith("#"));
//  }
//    
//  @Test
//  public void test_reading_leapseconds_from_relative_data_dir() {
//    String dir = "src/lasp/time/convert";
//    System.setProperty("data.dir", dir);
//    BufferedReader reader = LeapSecondUtil.getReaderFromDataDir();
//    String line = "";
//    try {
//        line = reader.readLine();
//    } catch (IOException e) {
//        e.printStackTrace();
//    } finally {
//        try {reader.close();} catch (Exception e) {}
//    }
//    
//    assert(line.startsWith("#"));
//  }
//    
//  @Test
//  public void test_reading_leapseconds_from_resource() {
//    BufferedReader reader = LeapSecondUtil.getReaderFromResource();
//    String line = "";
//    try {
//        line = reader.readLine();
//    } catch (IOException e) {
//        e.printStackTrace();
//    } finally {
//        try {reader.close();} catch (Exception e) {}
//    }
//    
//    assert(line.startsWith("#"));
//  }
}