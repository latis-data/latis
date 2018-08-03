package latis.util

import org.junit.After
import org.junit.Assert._
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Text
import latis.dm.TupleMatch

class TestCapabilities {
  
  /**
   * Cause the reload of properties before each test.
   */
  @After
  def resetProperties = LatisProperties.reset
  
  /**
   * Assert that the Capabilities Dataset is formatted correctly
   * and contains correct entries from the latis project's
   * test latis.properties file.
   */
  @Test
  def capabilities_dataset = {
    val ds = LatisCapabilities().getDataset 
    
    //latis.writer.AsciiWriter.write(ds)
    
    ds match {
      case Dataset(TupleMatch(Function(itDs), Function(itOut), Function(itOp))) => {
        itDs.next match { 
          case Sample(Index(i), Text(dsName)) => {
            assertEquals(0, i) 
            assertEquals("agg", dsName)
          }
        }
        
        itOut.next match {
          case Sample(Index(i), TupleMatch(Text(out), Text(desc))) => {
            assertEquals(0, i)
            assertEquals("asc", out)
            assertEquals("ASCII representation reflecting how the dataset is modeled.", desc)
          }
        }
        
        itOp.next match {
          case Sample(Index(i), TupleMatch(Text(op), Text(desc), Text(usage))) => {
            assertEquals(0, i)
            assertEquals("binave", op)
            assertEquals("Consolidate the data by binning (by a given width) and averaging the values in each bin.", desc)
            assertEquals("binave(width)", usage)
          }
        }
      }
      case _ => fail
    }
  }
 
  
}