package latis.reader

import org.junit._
import org.junit.Assert._
import latis.dm._
import latis.ops.Operation
import latis.ops.Projection
import latis.writer.AsciiWriter

class TestSystemReader {
  val sr = SystemReader()
  
  @Test
  def free: Unit = {   
    val totalMemory = sr.getTotal
    val freeMemory = sr.getFree
    
    assertTrue(totalMemory > freeMemory)
    assertEquals(totalMemory.getMetadata("name").get, "totalMemory")
    assertEquals(totalMemory.getMetadata("units").get, "MiB")
    assertEquals(freeMemory.getMetadata("name").get, "freeMemory")
    assertEquals(freeMemory.getMetadata("units").get, "MiB")
  }
  
  @Test
  def used: Unit = {   
    val totalMemory = sr.getTotal
    val usedMemory = sr.getUsed
    
    assertTrue(totalMemory >= usedMemory)
    assertEquals(usedMemory.getMetadata("name").get, "usedMemory")
    assertEquals(usedMemory.getMetadata("units").get, "MiB")
    
  }
  
  @Test
  def total: Unit = {   
    val totalMemory = sr.getTotal
    val maxMemory = sr.getMax
    
    assertTrue(maxMemory >= totalMemory)
    assertEquals(maxMemory.getMetadata("name").get, "maxMemory")
    assertEquals(maxMemory.getMetadata("units").get, "MiB")
  }
  
  @Test
  def percent: Unit = {   
    val percentUsed = sr.getPercentUsed
    
    assertTrue(percentUsed >= Integer(0))
    assertTrue(percentUsed <= Integer(100))
    assertEquals(percentUsed.getMetadata("name").get, "percentUsed")
    assertEquals(percentUsed.getMetadata("units").get, "percent")
  }
  
  @Test
  def dataSet: Unit = {  
    val ds = sr.getDataset()
    ds match {
      case Dataset(TupleMatch(free: Integer, used: Integer, total: Integer, percentUsed: Integer, max: Integer)) => {
        assertTrue(ds.getName == "memoryProperties")
        //assertEquals(free, sr.getFree) 
        //assertEquals(used, sr.getUsed)
        //assertEquals(total, sr.getTotal) 
        //assertEquals(percentUsed, sr.getPercentUsed)
        assertEquals(max, sr.getMax)   
      }
      case _ => fail  
    }
  }
    
  @Test
  def dataset_with_projections: Unit = {  
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += Projection("maxMemory")
    val ds = sr.getDataset(ops)
    ds match {
      case Dataset(max: Integer) => {
        assertEquals(max, sr.getMax)   
      }
      case _ => fail  
    }
  }
}