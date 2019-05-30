package latis.reader

import org.junit._
import Assert._
import latis.ops.Operation
import latis.ops.filter.Selection

class TestTimeRangeLimit {
  
  @Test
  def time_range_equals_limit = {
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += Selection("time >= 1970-01-01")
    ops += Selection("time <  1970-01-03")
    val ds = DatasetAccessor.fromName("ascii_with_limit").getDataset(ops)
    assertEquals(2, ds.getLength)
  }
  
  @Test(expected = classOf[UnsupportedOperationException])
  def time_range_exceeds_limit = {
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += Selection("time >= 1970-01-01")
    ops += Selection("time <  1970-01-03T00:00:01")
    val ds = DatasetAccessor.fromName("ascii_with_limit").getDataset(ops)
    assertEquals(2, ds.getLength)
  }
}