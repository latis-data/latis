package latis.reader

import org.junit.Test
import org.junit.Assert._
import latis.util.StringUtils
import latis.writer.AsciiWriter
import latis.writer.Writer
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.ops.filter.Selection

class TestDapReader {
  
  @Test
  def data {
    val url = StringUtils.getUrl("src/test/resources/datasets/test/dap/dap2")
    val r = new DapReader(url.toString)
    val ds = r.getDataset
    val data = ds.toDoubleMap
    assertEquals(2, data("myInt")(1), 0.0)
    assertEquals(3.3, data("myReal")(2), 0.0)
  }
  
  @Test
  def metadata {
    val url = StringUtils.getUrl("src/test/resources/datasets/test/dap/dap2")
    val r = new DapReader(url.toString)
    val ds = r.getDataset
    val t = ds.findVariableByName("myTime").get
    assertEquals("yyyy/MM/dd", t.getMetadata("units").get)
  }
  
  @Test
  def ops {
    val ops = ArrayBuffer[Operation]()
    ops += Selection("myInt>1")
    val url = StringUtils.getUrl("src/test/resources/datasets/test/dap/dap2")
    val r = new DapReader(url.toString)
    val ds = r.getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(2, data("myInt").length)
  }
    
}