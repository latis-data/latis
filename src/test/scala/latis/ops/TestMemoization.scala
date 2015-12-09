package latis.ops

import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.dm.TestDataset
import latis.writer.AsciiWriter

class TestMemoization {
  
  @Test
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
  
  @Test
  def length_metadata = {
    val ds = TestDataset.numeric_time_series
    assert(ds.unwrap.getMetadata.isEmpty)
    val ds2 = ds.force
    assertEquals("3", ds2.unwrap.getMetadata("length").get)
  }
  
  @Test
  def nested_function_not_memoized = {
    val ds = TestDataset.function_of_functions2
    ds.toDoubleMap //traverse once
    val map = ds.toDoubleMap //traverse twice
    assert(map.isEmpty)
  }
  
  @Test
  def nested_function_memoized = {
    val ds = TestDataset.function_of_functions2.force
    ds.toDoubleMap //traverse once
    val map = ds.toDoubleMap //traverse twice
    assert(map.nonEmpty)
  }
}