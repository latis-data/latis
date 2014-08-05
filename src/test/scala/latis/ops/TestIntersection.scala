package latis.ops

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer._
import latis.ops.agg.CollectionAggregation
import latis.ops.agg.TileAggregation
import latis.dm._
import latis.ops.agg.Intersection

class TestIntersection {
  
  val ds1 = Dataset(Function.fromValues(Seq(1,2,4), Seq(1,2,3)))
  val ds2 = Dataset(Function.fromValues(Seq(1,3,4), Seq(4,5,6)))
  
  @Test
  def same_ds {
    val op = new Intersection()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    val data = ds.toDoubles
    assertEquals(2, data(0).length) //# domain values
    assertEquals(4, data(1).length) //# range values
    assertEquals(6.0, data(1)(3), 0.0)
  }
}