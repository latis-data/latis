package latis.reader

import org.junit._
import Assert._

class TestColumnarAdapter extends AdapterTests {
  def datasetName = "col"
  
  @Test
  def invalid_row = {
    val ds = DatasetAccessor.fromName("col").getDataset().force
    assertEquals(3, ds.getLength)
  }
}
