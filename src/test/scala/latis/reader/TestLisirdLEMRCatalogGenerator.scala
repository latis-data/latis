package latis.reader

import latis.reader.tsml._
import org.junit._
import Assert._
import latis.writer.AsciiWriter
import java.net.URL
import latis.ops._

class TestLisirdLEMRCatalogGenerator {
  
  @Test
  def check_lemr_catalog = {
    val ds = LisirdLemrCatalogGenerator().getDataset 

    AsciiWriter.write(ds)
    assertEquals(39, ds.getLength) //This number could change if LEMR changes
  }
  
  
}
