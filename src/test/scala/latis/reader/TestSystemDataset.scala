package latis.reader

import org.junit.Test
import org.junit.Assert._
import latis.dm._

class TestSystemDataset {
  val sr = SystemReader.apply
  
  @Test
  def free {   
    val totalMemory = sr.getTotal
    val freeMemory = sr.getFree
    
    assertTrue(totalMemory > freeMemory)
  }
  
  @Test
  def used {   
    val totalMemory = sr.getTotal
    val used = sr.getUsed
    
    assertTrue(totalMemory >= used)
  }
  
  @Test
  def total {   
    val totalMemory = sr.getTotal
    val max = sr.getMax
    
    assertTrue(max >= totalMemory)
  }
  
  @Test
  def percent {   
    val percentUsed = sr.getPercentUsed
    
    assertTrue(percentUsed >= Integer(0))
    assertTrue(percentUsed <= Integer(100))
  }
  
  @Test
  def dataSet {   
    val ds = sr.getDataset
    val scalars = ds.getScalars
    assertTrue(ds.getName == "memoryProperties")
    assertEquals(scalars(0), sr.getFree) 
    assertEquals(scalars(1), sr.getUsed)
    assertEquals(scalars(2), sr.getTotal) 
    assertEquals(scalars(3), sr.getPercentUsed)
    assertEquals(scalars(4), sr.getMax) 
  }
  
}