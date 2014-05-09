package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.metadata.Metadata
import latis.ops.filter.Selection
import latis.data.seq.DataSeq
import latis.data.Data
import latis.data.SampledData

class TestSelection {
  
  // time -> value
  lazy val scalarFunction: Function = {
    val domain = Real(Metadata("time"))
    val range = Real(Metadata("value"))
    val data = SampledData.fromValues(Seq(1,2,3,4,5), Seq(1,2,3,4,5))
    Function(domain, range, data=data)
  }
  
  //TODO: run same tests against multiple Functions
  def testSelection(f: Function, s: String, expected: Function) {
    val filter = Selection(s)
    val ds = filter(f)
    //AsciiWriter().write(expected)
    //AsciiWriter().write(ds)
    assert(expected equals ds.getVariables(0))
  }

  
  @Test
  def select_equal_value {
    val domain = Real(Metadata("time"))
    val range = Real(Metadata("value"))
    val data = SampledData.fromValues(Seq(3,5), Seq(3,3))
    val expected = Function(domain, range, data=data)
    testSelection(scalarFunction, "value=3", expected)
  }
  
  @Test
  def select_gt_value {
    val filter = Selection("value>2")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(3, n)
  }
  
  @Test
  def select_lt_value {
    val filter = Selection("value<3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(2, n)
  }
  
  @Test
  def select_ge_value {
    val filter = Selection("value>=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(3, n)
  }
  
  @Test
  def select_le_value {
    val filter = Selection("value<=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(3, n)
  }
  
  @Test
  def select_ne_value {
    val filter = Selection("value!=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(4, n)
  }
  
  @Test
  def select_equal_time {
    val filter = Selection("time=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(1, n)
  }
  
  @Test
  def select_twice {
    val ds = Dataset(scalarFunction)
    val ds2 = ds.filter("value >= 2")
    val ds3 = ds2.filter("value < 5")
    val n = ds3.length
    assertEquals(3, n)
  }
  
}




