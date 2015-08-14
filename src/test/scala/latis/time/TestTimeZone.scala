package latis.time

import org.junit._
import Assert._
import latis.metadata.Metadata
import latis.dm.Real
import latis.dm.Number
import latis.util.LatisProperties
import java.util.TimeZone

class TestTimeZone {

  //TODO: need mock LatisProperties?
  
  @Test
  def time_zone_property = {
    val tz = LatisProperties.get("time.zone")
    assert(tz.isEmpty)
    assertEquals("UTC", TimeZone.getDefault.getID)
  }

  // @Test  //Add to src/test/resources/latis.properties: time.zone = America/Denver
  def other_time_zone_property = {
    val tz = LatisProperties("time.zone")
    assertEquals("America/Denver", tz)
    assertEquals("America/Denver", TimeZone.getDefault.getID)
  }
}
