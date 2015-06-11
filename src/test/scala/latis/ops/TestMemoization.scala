package latis.ops

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.dm.TestDataset
import latis.writer.AsciiWriter

class TestMemoization {
  
  @Test @Ignore //the orig dataset is left unusable (LATIS-306)
  def immutability = {
    val op = TimeFormatter("yyyy-MM-dd")
    val ds = TestDataset.numeric_time_series.force
    //days since 2000-01-01
    
    val data = ds.toStringMap
    assertEquals(data("myTime").length, 3)
    
    val ds2 = op(ds) //apply operation
    
    val data1 = ds.toStringMap
    assertEquals(data1("myTime").length, 3) //orig ds should still have same length

    val data2 = ds2.toStringMap
    assertEquals(data2("myTime").length, 3) //op didn't change number of samples
  }
  
}