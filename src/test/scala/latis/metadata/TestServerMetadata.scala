package latis.metadata

import org.junit._
import Assert._

class TestServerMetadata {
  
  @Test
  def testCsvDescription {
    val csvInfoOpt = ServerMetadata.availableSuffixes.find(
      suffixInfo => suffixInfo.suffix.equals("csv")
    )
    assert(!csvInfoOpt.isEmpty, "Could not find suffix 'csv'")
    
    val csvInfo = csvInfoOpt.get
    assertEquals("csv", csvInfo.suffix)
    assertEquals("CsvWriter", csvInfo.className)
    assertEquals("ASCII comma separated values", csvInfo.description)
  }
  
  @Test
  def testFirstFilter {
    val firstOpOpt = ServerMetadata.availableOperations.find(
        opInfo => opInfo.name.equals("first")
    )
    assert(!firstOpOpt.isEmpty, "Could not find Operation 'first'")
    
    val firstOp = firstOpOpt.get
    assertEquals("first", firstOp.name)
    assertEquals("FirstFilter", firstOp.className)
    assertEquals("return only the first sample", firstOp.description)
  }

}