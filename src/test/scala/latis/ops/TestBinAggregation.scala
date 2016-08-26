package latis.ops

import org.junit.Test
import org.junit.Assert._
import latis.dm.Dataset
import latis.dm.TestDataset
import latis.writer.AsciiWriter
import latis.reader.tsml.TsmlReader

class TestBinAggregation {
  
  @Test
  def count {
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val op = BinAggregation(BinAggregation.COUNT, 3)
    val data = op(ds).toDoubles
    assertEquals(2, data.length)
    assertEquals(4, data(0).length)
    assertEquals(3.0, data(1)(1), 0)
    assertEquals(1.0, data(1)(3), 0)
  }
  
  @Test
  def sum {
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val op = BinAggregation(BinAggregation.SUM, 3)
    val data = op(ds).toDoubles
    assertEquals(2, data.length)
    assertEquals(4, data(0).length)
    assertEquals(3.0, data(0)(0), 0)
    assertEquals(19.0, data(1)(3), 0)
  }
  
  @Test
  def min {
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val op = BinAggregation(BinAggregation.MIN, 3)
    val data = op(ds).toDoubles
    assertEquals(2, data.length)
    assertEquals(4, data(0).length)
    assertEquals(0.0, data(0)(0), 0)
    assertEquals(16.0, data(1)(2), 0)
  }
  
  @Test
  def max {
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val op = BinAggregation(BinAggregation.MAX, 3)
    val data = op(ds).toDoubles
    assertEquals(2, data.length)
    assertEquals(4, data(0).length)
    assertEquals(2.0, data(0)(0), 0)
    assertEquals(18.0, data(1)(2), 0)
  }
  
  @Test
  def average {
    val ds = TsmlReader("datasets/test/scalar.tsml").getDataset
    val op = BinAggregation(BinAggregation.AVERAGE, 3)
    val it = op(ds) match {
      case Dataset(v) => v.findFunction.get.iterator
      case _ => null
    }
    assertEquals(1.0, it.next.domain.getNumberData.doubleValue, 0)
    assertEquals(14.0, it.next.range.getNumberData.doubleValue, 0)
  }

}
