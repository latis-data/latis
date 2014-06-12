package latis.ops

import latis.dm.Function
import latis.dm.Real
import latis.metadata.Metadata
import latis.data.SampledData
import org.junit._
import Assert._
import latis.ops.filter._
import latis.dm.Dataset

class TestStrideFilter {
  
  val dataset = Dataset(Function.fromValues(List(1.1,2.2,3.3), List(4.4,5.5,6.6)))

  @Test
  def test_stride{
    val ds = StrideFilter(2)(dataset).toDoubles
    assertEquals(1.1, ds(0)(0), 0.0)
    assertEquals(3.3, ds(0)(1), 0.0)
  }
  
  @Test
  def test_length {
    val ds = StrideFilter(4)(dataset)
    assertEquals(1, ds.length)
    val ds2 = StrideFilter(3)(dataset)
    assertEquals(1, ds2.length)
    val ds3 = StrideFilter(2)(dataset)
    assertEquals(2, ds3.length)
    val ds4 = StrideFilter(1)(dataset)
    assertEquals(3, ds4.length)
  }

}