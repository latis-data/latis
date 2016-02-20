package latis.ops

import latis.dm.Function
//import latis.dm.implicits._
import latis.dm.Real
import latis.metadata.Metadata
import latis.data.SampledData
import org.junit._
import Assert._
import latis.ops.filter._
import latis.dm.Dataset
import latis.dm.TestDataset
import latis.dm.Sample
import latis.writer.Writer
import latis.dm.Integer
import latis.dm.Tuple
import latis.dm.Text
import latis.dm.Index
import latis.time.Time

class TestMaxFilter {
  
  @Test
  def test_canonical = {
    val ds1 = TestDataset.canonical
    val ds2 = MaxFilter("myInt")(ds1)
    latis.writer.AsciiWriter.write(ds2) 
  }
  
}