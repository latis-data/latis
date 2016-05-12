package latis.ops

import latis.dm._
import latis.metadata._
import org.junit._
import Assert._
import latis.ops._
import latis.ops.filter._
import latis.writer._

class TestMaxDeltaFilter {
  
  @Test
  def one_bad_value = {
    val sample1 = Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2))
    val sample3 = Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5))
    val sample5 = Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 8.9))
    val sample6 = Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4))
    val sample7 = Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 3.1))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    
    val ds2 = MaxDeltaFilter(2.0)(ds1)
    latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        it.drop(4).next match { 
          case Sample(Integer(t), Real(v)) => assertEquals((6, 3.4), (t, v))
        }
      }
    }
  }
  
}