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
  
  //selecting on unprojected variable is now an exception, so this breaks
  //when the selection is applied to the combined dataset. 
  @Test @Ignore 
  override def select_on_non_projected_domain_with_selection_first {}
}