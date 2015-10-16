package latis.reader

import org.junit.Test
import latis.util.StringUtils
import latis.writer.AsciiWriter

class TestDapReader {
  
  //@Test
  def getDataset {
    val url = "http://localhost:8080/lisird3/latis/historical_tsi"
    val r = new DapReader(url)
    val ds = r.getDataset
    AsciiWriter.write(ds)
  }
  
}