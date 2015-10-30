package latis.ops

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer._
import latis.ops.agg.CollectionAggregation
import latis.ops.agg.TileAggregation
import latis.dm._
import latis.ops.agg.Intersection
import latis.metadata.Metadata

class TestIntersection {
  
  
  @Test
  def scalar_functions {
    val ds1 = Dataset(Function.fromValues(Seq(1,2,4), Seq(1,2,3)))
    val ds2 = Dataset(Function.fromValues(Seq(1,3,4), Seq(4,5,6)))
    val op = new Intersection()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    val data = ds.toDoubles
    assertEquals(2, data(0).length) //# domain values
    assertEquals(4, data(1).length) //# range values
    assertEquals(6.0, data(1)(3), 0.0)
  }
  
  @Test
  def metadata_preservation = {
    val samples1 = (0 to 3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = (0 to 3).map(i => Sample(Real(Metadata("t"), i+1), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val md = Metadata("dataset3")
    val ds3 = Intersection(ds1, ds2, md)
    //latis.writer.AsciiWriter.write(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    //latis.writer.AsciiWriter.write(ds3)
    assertEquals("dataset3", ds3.getName)
    ds3 match {
      case Dataset(f: Function) => assertEquals("function1", f.getName) //uses Function from first dataset for now
    }
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