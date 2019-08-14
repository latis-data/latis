package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Text
import latis.ops.filter.DailyNearestToNoon
import latis.reader.DatasetAccessor

class TestDailyNearestToNoon {

  @Test
  def test_utc_noon: Unit = {
    val ops = List(
      TimeFormatter("yyyy-MM-dd'T'HH:mm:ss"),
      DailyNearestToNoon()
    )
    val ds = DatasetAccessor.fromName("tsi_with_time_of_day").getDataset(ops)
    //latis.writer.AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Text(time), Real(tsi)) =>
          assertEquals("2000-03-08T11:30:00", time)
          assertEquals(1360.4933, tsi, 0)
      }
    }
  }
  
  @Test
  def test_offset_noon: Unit = {
    val ops = List(
      TimeFormatter("yyyy-MM-dd'T'HH:mm:ss"),
      DailyNearestToNoon(-8.0)
    )
    val ds = DatasetAccessor.fromName("tsi_with_time_of_day").getDataset(ops)
    //latis.writer.AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Text(time), Real(tsi)) =>
          assertEquals("2000-03-08T19:30:00", time)
          assertEquals(1360.4667, tsi, 0)
      }
    }
  }
  
}
