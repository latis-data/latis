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
  }
  
  @Test
  def used: Unit = {   
    val totalMemory = sr.getTotal
    val used = sr.getUsed
    
    assertTrue(totalMemory >= used)
  }
  
  @Test
  def total: Unit = {   
    val totalMemory = sr.getTotal
    val max = sr.getMax
    
    assertTrue(max >= totalMemory)
  }
  
  @Test
  def percent: Unit = {   
    val percentUsed = sr.getPercentUsed
    
    assertTrue(percentUsed >= Integer(0))
    assertTrue(percentUsed <= Integer(100))
  }
  
  @Test
  def dataSet: Unit = {  
    val ds = sr.getDataset()
    ds match {
      case Dataset(TupleMatch(free: Integer, used: Integer, total: Integer, percentUsed: Integer, max: Integer)) => {
        assertTrue(ds.getName == "memoryProperties")
        assertEquals(free, sr.getFree) 
        assertEquals(used, sr.getUsed)
        assertEquals(total, sr.getTotal) 
        assertEquals(percentUsed, sr.getPercentUsed)
        assertEquals(max, sr.getMax)   
      }
      case _ => fail  
    }
  }
    
  @Test
  @Ignore
  def dataset_with_projections: Unit = {  
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += Projection("percentUsed")
    val ds = sr.getDataset(ops)
    AsciiWriter.write(ds)
    ds match {
      case Dataset(percentUsed: Integer) => {
        assertEquals(percentUsed, sr.getPercentUsed)
      }
      case _ => fail  
    }
  }
}