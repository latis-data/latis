package latis.metadata

import org.junit._
import Assert._

class TestServerMetadata {
  
  @Test
  def testCsvDescription {
    val csvInfoOpt = ServerMetadata.availableSuffixes.find(
      suffixInfo => suffixInfo.suffix.equals("csv")
    )
    assert(!csvInfoOpt.isEmpty)
    
    val csvInfo = csvInfoOpt.get
    assertEquals(csvInfo.suffix, "csv")
    assertEquals(csvInfo.className, "latis.writer.CsvWriter")
    assertEquals(csvInfo.description, "ASCII comma separated values")
  }

}