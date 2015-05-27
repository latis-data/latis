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
  
  
  @Test
  def scalar_functions {
    val ds1 = Dataset(Function.fromValues(Seq(1,2,4), Seq(1,2,3)))
    val ds2 = Dataset(Function.fromValues(Seq(1,3,4), Seq(4,5,6)))
    val op = new Intersection()
    val ds = op(Dataset(Tuple(ds1.unwrap, ds2.unwrap)))
    //AsciiWriter.write(ds)
    val data = ds.toDoubles
    assertEquals(2, data(0).length) //# domain values
    assertEquals(4, data(1).length) //# range values
    assertEquals(6.0, data(1)(3), 0.0)
  }
  
  //@Test
  def tuple_functions {
    val ds1 = TsmlReader("/home/lindholm/git/webtcad-mms/src/test/resources/datasets/Schemas.tsml").getDataset
    val ds2 = TsmlReader("/home/lindholm/git/webtcad-mms/src/test/resources/datasets/SchemasTimeRange.tsml").getDataset
    
 //   AsciiWriter.write(ds1)
 //   AsciiWriter.write(ds2)
    val ds = ds1.intersect(ds2) //iterable once problem? .reduce
    AsciiWriter.write(ds)
  }
  
  //@Test
  def intersection_adapter {
    val ds = TsmlReader("/home/lindholm/git/webtcad-mms/src/test/resources/datasets/Schemas2.tsml").getDataset
    AsciiWriter.write(ds)
  }
}