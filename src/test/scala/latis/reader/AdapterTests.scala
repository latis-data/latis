package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.ops.filter.Selection
import latis.ops.Projection
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.dm.Dataset
import latis.util.DataMap
import latis.ops.filter._

abstract class AdapterTests {
  
  /**
   * Override this to define the name of the "dataset" you want to test.
   * This will look for the descriptor as "datasets/test/<datasetName>.tsml"
   * It is expected that each adapted dataset represent the same values.
   */
  def datasetName: String
  
  def getDataset(ops: Seq[Operation]) = TsmlReader("datasets/test/" + datasetName + ".tsml").getDataset(ops)
  def getDataset: Unit = getDataset(Seq[Operation]())
  
  def writeDataset(ops: Seq[Operation]) = AsciiWriter().write(getDataset(ops))
  def writeDataset: Unit = writeDataset(Seq[Operation]())
  
  //---- Test Projection ----------------------------------------------------//
  
  @Test
  def project_all {
    val ops = List(Projection("myTime, myInt, myReal, myText"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length) //got all samples
    assertEquals(4, data.keySet.size)    //4 projected variables
  }
  
  @Test
  def project_domain_and_one_range {
    //TODO: reduce unnamed tuple of one
    val ops = List(Projection("myTime, myInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length) //got all samples
    assertEquals(2, data.keySet.size)    //2 projected variables
  }
  
  @Test
  def project_domain_and_two_range {
    val ops = List(Projection("myTime, myInt, myReal"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length) //got all samples
    assertEquals(3, data.keySet.size)    //3 projected variables
  }
  
  //@Test
  //TODO: projection order not yet supported
  def project_diff_order {
    val ops = List(Projection("myTime, myReal, myInt"))
    val ds = getDataset(ops)
    assertEquals("myInt", ds.toSeq(2).getName)
  }
  
  @Test
  def project_without_domain {
    val ops = List(Projection("myInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myInt").length)
    assertEquals(2, data.keySet.size) //index and time
  }
  
  @Test
  def project_only_domain {
    val ops = List(Projection("myTime"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("myTime").length)
    assertEquals(2, data.keySet.size) //index and time
  }
  
  //---- Test Selection -----------------------------------------------------//
  
  @Test
  def select_on_domain {
    //Note, domain is Time as Text with format yyyy/MM/dd but ISO form works
    //TODO: support time selection with native format: "time > 1970/01/01"
    val ops = List(Selection("time > 1970-01-01"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assertEquals(2, data("myInt").length)
    assertEquals(2, data("myReal").length)
    assertEquals(2, data("myText").length)
    assertEquals(2, data("myInt").head.toInt)
  }
  
  @Test
  def select_on_one_range_value {
    val ops = List(Selection("myInt >= 2"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myInt").length)
    assertEquals(2, data("myInt").head.toInt)
  }
  
  @Test
  def select_on_two_range_values { //TODO: misnamed or wrong test
    val ops = List(Selection("time > 1970-01-01"), Selection("myInt <= 3"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assertEquals(2, data("myInt").head.toInt)
  }
  
  @Test
  def two_selects_on_domain {
    val ops = List(Selection("time > 1970-01-01"), Selection("time <= 1970-01-03"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assertEquals(2, data("myInt").head.toInt)
  }
  
  @Test
  def two_selects_on_range_value {
    val ops = List(Selection("myInt > 1"), Selection("myInt <= 3"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myInt").length)
    assertEquals(2, data("myInt").head.toInt)
  }
  
  @Test
  def string_match {
    val ops = List(Selection("myText =~ B"))
    val data = getDataset(ops).toStringMap
    assertEquals(1, data("myText").length)
    assertEquals(2, data("myInt").head.toInt)
  }
  
  //TODO: test various equals, match,... or leave to Operation tests?
  
  //---- Test Filters -------------------------------------------------------//
  
  @Test
  def first {
    val ops = List(FirstFilter())
    val data = getDataset(ops).toStringMap
    assertEquals(1, data("myTime").length)
    assertEquals(1, data("myInt").head.toInt)
  }  
  
  @Test
  def last {
    val ops = List(LastFilter())
    val data = getDataset(ops).toStringMap
    assertEquals(1, data("myTime").length)
    assertEquals(3, data("myInt").head.toInt)
  }
  
  @Test
  def limit {
    val ops = List(LimitFilter(2))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myTime").length)
    assertEquals(1, data("myInt").head.toInt)
  }
  
  
  //---- Test Combined Operations -------------------------------------------//
  //TODO: lots of permutations
    
  @Test
  def projection_before_selection {
    val ops = List(Projection("myTime, myReal"), Selection("myReal > 2"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //2 projected variables
    assertEquals(2, data("myTime").length)
    assertEquals(2.2, data("myReal").head.toDouble, 0.0)
  }
  
  @Test
  def selection_before_projection {
    val ops = List(Selection("myReal > 2"), Projection("myTime, myReal"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //2 projected variables
    assertEquals(2, data("myTime").length)
    assertEquals(2.2, data("myReal").head.toDouble, 0.0)
  }
  
  //@Test
  //TODO: ignores selection, works for JdbcAdapter
  def select_on_non_projected_domain_with_projection_first {
    val ops = List(Projection("myReal"), Selection("time < 1970-01-02"))
    val data = getDataset(ops).toDoubleMap
    assertEquals(2, data.keySet.size)  //index and myReal
    assertEquals(1, data("myReal").length)
    assertEquals(1.1, data("myReal").head, 0.0)
  }
  
  @Test
  def select_on_non_projected_domain_with_selection_first {
    val ops = List(Selection("time < 1970-01-02"), Projection("myReal"))
    val data = getDataset(ops).toDoubleMap
    assertEquals(2, data.keySet.size)  //index and myReal
    assertEquals(1, data("myReal").length)
    assertEquals(1.1, data("myReal").head, 0.0)
  }
  
  @Test
  def select_on_index_when_no_projected_domain {
    val ops = List(Projection("myReal"), Selection("index > 1"))
    val ds = getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(2, data.keySet.size)  //index and myReal
    assertEquals(1, data("index").length)
    assertEquals(2, data("index").head, 0.0)
    assertEquals(3.3, data("myReal").head, 0.0)
  }
  
  @Test
  def select_on_index_when_no_projected_range {
    val ops = List(Projection("myTime"), Selection("index = 1"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)  //index and time
    assertEquals(1, data("index").length)
    assertEquals(1, data("index").head.toInt)
  }
}
