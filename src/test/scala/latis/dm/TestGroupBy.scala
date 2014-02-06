package latis.dm

import org.junit._
import Assert._
import latis.writer.AsciiWriter
import latis.metadata.Metadata

class TestGroupBy {

  //@Test
  def group_by_domain {
    //should have no change
    val ds = TestDataset.function_of_tuple_with_mixed_types.groupBy("myInteger")
    AsciiWriter.write(ds)
  }
  
  //@Test
  def change_domain {
    val ds = TestDataset.function_of_tuple_with_mixed_types.groupBy("myReal")
    //val ds = TestDataset.function_of_tuple_with_mixed_types.groupBy("myText")
    AsciiWriter.write(ds)
  }
  
  //TODO: duplicate values
  @Test
  def duplicate_values {
    //Function(List(1.0,2.0,3.0,4.0,5.0), List(1.0,0.0,1.0,0.0,1.0), List(11.0,22.0,33.0,44.0,55.0))
    val samples = List(Sample(Integer(Metadata("myInteger"), 0), Tuple(Real(Metadata("myReal"), 0), Text(Metadata("myText"), "A"))), 
                       Sample(Integer(Metadata("myInteger"), 1), Tuple(Real(Metadata("myReal"), 1), Text(Metadata("myText"), "B"))), 
                       Sample(Integer(Metadata("myInteger"), 2), Tuple(Real(Metadata("myReal"), 2), Text(Metadata("myText"), "A"))), 
                       Sample(Integer(Metadata("myInteger"), 3), Tuple(Real(Metadata("myReal"), 3), Text(Metadata("myText"), "B"))), 
                       Sample(Integer(Metadata("myInteger"), 4), Tuple(Real(Metadata("myReal"), 4), Text(Metadata("myText"), "A"))))
    val ds = Dataset(Function(samples)).groupBy("myText")
    AsciiWriter.write(ds)
  }
}