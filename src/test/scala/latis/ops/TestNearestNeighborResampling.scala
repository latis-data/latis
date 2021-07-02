package latis.ops

import org.junit.Test
import org.junit.Assert._
import latis.dm._
import latis.data.Data
import latis.writer.AsciiWriter
import latis.ops.resample.NearestNeighborResampling
import latis.metadata.Metadata

class TestNearestNeighborResampling {
  
  @Test
  def resample {
    val dvar = Real(Metadata("myTime"),0.0)
    val dset = Seq(dvar(Data(0.0)), dvar(Data(8E7)), dvar(Data(16E7)) )
    val op = new NearestNeighborResampling(dset)
    val ds = op(TestDataset.canonical)
    val data = ds.toStrings
    assertEquals("1", data(1)(0))
    assertEquals("2.2", data(2)(1))
    assertEquals("3", data(1)(2))
    assertEquals("NaN", data(3)(0))
    assertEquals("8.0E7", data(0)(1))
  }
  
  @Test
  def out_of_bounds {
    val dvar = Real(Metadata("myTime"),0.0)
    val dset = Seq(dvar(Data(-1.0)), dvar(Data(1E10)) )
    val op = new NearestNeighborResampling(dset)
    val ds = op(TestDataset.canonical)
    val data = ds.toStrings
    assertEquals("1", data(1)(0))
    assertEquals("3", data(1)(1))
  }
  
  @Test
  def centered {
    val dvar = Real(Metadata("myTime"),0.0)
    val dset = Seq(dvar(Data(43200000-.1)), dvar(Data(43200000.0)), dvar(Data(43200000.1)) )
    val op = new NearestNeighborResampling(dset)
    val ds = op(TestDataset.canonical)
    val data = ds.toStrings
    assertEquals("1", data(1)(0))
    assertEquals("2", data(1)(1))
    assertEquals("2", data(1)(2))
  }
  
}
