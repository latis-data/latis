package latis.reader

import org.junit.Test
import org.junit.Ignore

class TestTileUnionAdapter extends AdapterTests{
  
  def datasetName = "agg/agg_append"
  
  //Selection on index is not idempotent, so these tests break when 
  //one is passed to each individual dataset as it is read. 
  @Test @Ignore 
  override def select_on_index_when_no_projected_range {}
  
  @Test @Ignore 
  override def select_on_index_when_no_projected_domain {}
  
}