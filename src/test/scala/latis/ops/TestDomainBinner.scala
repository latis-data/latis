package latis.ops

import scala.collection.mutable.ArrayBuffer

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.TsmlReader

class TestDomainBinner {
  
  @Test
  def start {
    val ops = ArrayBuffer[Operation]()
    ops += DomainBinner("start", "10")
    val ds = TsmlReader("scalar.tsml").getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(10, data("end_foo").last, 0.0)
    assertEquals(0.0, data("start_foo").head, 0.0)
  }
  
  @Test
  def end {
    val ops = ArrayBuffer[Operation]()
    ops += DomainBinner("end", "-1")
    val ds = TsmlReader("scalar.tsml").getDataset(ops)
    val data = ds.toDoubleMap
    assertEquals(9, data("end_foo").last, 0.0)
    assertEquals(-1, data("start_foo").head, 0.0)
  }

}