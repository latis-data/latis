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

  //This test will work once the iterable once problem is solved
  //@Test
  def test_stride{
    val ds = StrideFilter(2)(dataset).toDoubles
    assertEquals(1.1, ds(0)(0), 0.0)
    assertEquals(3.3, ds(0)(1), 0.0)
  }

}