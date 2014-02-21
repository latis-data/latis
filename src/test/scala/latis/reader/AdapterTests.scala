package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.ops.Selection
import latis.ops.Projection
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.dm.Dataset
import latis.util.DataMap

abstract class AdapterTests {
  
  /**
   * Override this to define the name of the "dataset" you want to test.
   * This will look for the descriptor as "datasets/test/<datasetName>.tsml"
   * It is expected that each adapted dataset represent the same values.
   */
  def datasetName: String
  def getDataset(ops: Seq[Operation]) = TsmlReader("datasets/test/" + datasetName + ".tsml").getDataset(ops)
  def writeDataset(ops: Seq[Operation]) = AsciiWriter().write(getDataset(ops))
  
  //---- Test Projection ----------------------------------------------------//
  
  @Test
  def project_all {
    val ops = List(Projection("time, int, real, text"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("time").length) //got all samples
    assertEquals(4, data.keySet.size)    //4 projected variables
  }
  
  @Test
  def project_domain_and_one_range {
    //TODO: reduce unnamed tuple of one
    val ops = List(Projection("time, int"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("time").length) //got all samples
    assertEquals(2, data.keySet.size)    //2 projected variables
  }
  
  @Test
  def project_domain_and_two_range {
    val ops = List(Projection("time, int, real"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("time").length) //got all samples
    assertEquals(3, data.keySet.size)    //3 projected variables
  }
  
  @Test
  //TODO: projection order not yet supported
  def project_diff_order {
    val ops = List(Projection("time, real, int"))
    val ds = getDataset(ops)
    assertEquals("int", ds.toSeq(2).getName)
  }
  
  @Test
  def project_without_domain {
    val ops = List(Projection("int"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("int").length)
    assertEquals(2, data.keySet.size) //index and time
  }
  
  @Test
  def project_only_domain {
    val ops = List(Projection("time"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("time").length)
    assertEquals(2, data.keySet.size) //index and time
  }
  
  //---- Test Selection -----------------------------------------------------//
  
  @Test
  def select_on_domain {
    //Note, domain is Time as Text with format yyyy/MM/dd but ISO form works
    //TODO: support time selection with native format: "time > 1970/01/01"
    val ops = List(Selection("time > 1970-01-01"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("time").length)
    assertEquals(2, data("int").head.toInt)
  }
  
  @Test
  def select_on_one_range_value {
    val ops = List(Selection("int >= 2"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("int").length)
    assertEquals(2, data("int").head.toInt)
  }
  
  @Test
  def select_on_two_range_values {
    val ops = List(Selection("time > 1970-01-01"), Selection("int <= 3"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("time").length)
    assertEquals(2, data("int").head.toInt)
  }
  
  @Test
  def two_selects_on_domain {
    val ops = List(Selection("time > 1970-01-01"), Selection("time <= 1970-01-03"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("time").length)
    assertEquals(2, data("int").head.toInt)
  }
  
  @Test
  def two_selects_on_range_value {
    val ops = List(Selection("int > 1"), Selection("int <= 3"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("int").length)
    assertEquals(2, data("int").head.toInt)
  }
  
  //TODO: test various equals, match,... or leave to Operation tests?
  
  //---- Test Combined Operations -------------------------------------------//
    
  @Test
  def projection_before_selection {
    val ops = List(Projection("time, real"), Selection("real > 2"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //2 projected variables
    assertEquals(2, data("time").length)
    assertEquals(2.2, data("real").head.toDouble, 0.0)
  }
  
  @Test
  def selection_before_projection {
    val ops = List(Selection("real > 2"), Projection("time, real"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //2 projected variables
    assertEquals(2, data("time").length)
    assertEquals(2.2, data("real").head.toDouble, 0.0)
  }
  
  @Test
  //TODO: ignores selection
  def select_on_non_projected_domain {
    val ops = List(Projection("real"), Selection("time < 1970-01-02"))
    val data = getDataset(ops).toDoubleMap
    assertEquals(2, data.keySet.size)  //index and real
    assertEquals(1, data("real").length)
    assertEquals(1.1, data("real").head, 0.0)
  }
  
  @Test
  def select_on_index_when_no_projected_domain {
    val ops = List(Projection("real"), Selection("index > 1"))
    val data = getDataset(ops).toDoubleMap
    assertEquals(2, data.keySet.size)  //index and real
    assertEquals(1, data("index").length)
    assertEquals(2, data("index").head, 0.0)
    assertEquals(3.3, data("real").head, 0.0)
  }
  
  @Test
  def select_on_index_when_no_projected_range {
    val ops = List(Projection("time"), Selection("index = 1"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //index and time
    assertEquals(1, data("index").length)
    assertEquals(1, data("index").head.toInt)
  }
}
