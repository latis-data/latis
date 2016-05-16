package latis.ops

import org.junit.Assert._
import org.junit.Test

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.ops.filter.MaxDeltaFilter

class TestMaxDeltaFilter {
  
  @Test
  def one_bad_value = { // t -> v
    val sample1 = Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2))
    val sample3 = Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5))
    val sample5 = Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 8.9)) //"bad" value
    val sample6 = Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4))
    val sample7 = Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 3.1))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    val ds2 = MaxDeltaFilter("v", 2.0)(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        val filterSamples = it.toSeq
          
        assertEquals(Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5)), filterSamples(0))
        assertEquals(Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2)), filterSamples(1))
        assertEquals(Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6)), filterSamples(2))
        assertEquals(Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5)), filterSamples(3))
        assertEquals(Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4)), filterSamples(4))
        assertEquals(Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 3.1)), filterSamples(5))
      }
    }
  }
  
  @Test
  def three_bad_values = { // t -> v
    val sample1 = Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2))
    val sample3 = Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 12.4)) //"bad" value
    val sample5 = Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 8.9))  //"bad" value
    val sample6 = Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 9.6))  //"bad" value
    val sample7 = Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 3.1))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    val ds2 = MaxDeltaFilter("v", 1.0)(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        val filterSamples = it.toSeq
        
        assertEquals(Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5)), filterSamples(0))
        assertEquals(Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2)), filterSamples(1))
        assertEquals(Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6)), filterSamples(2))
        assertEquals(Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 3.1)), filterSamples(3))
      }
    }
  }
  
  @Test
  def two_bad_values_apart = { // t -> v
    val sample1 = Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 9000.0)) //"bad" value
    val sample3 = Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5))
    val sample5 = Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 3.9))
    val sample6 = Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4))
    val sample7 = Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 16.1))   //"bad" value
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    val ds2 = MaxDeltaFilter("v", 2.0)(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        val filterSamples = it.toSeq
          
        assertEquals(Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5)), filterSamples(0))
        assertEquals(Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6)), filterSamples(1))
        assertEquals(Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5)), filterSamples(2))
        assertEquals(Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 3.9)), filterSamples(3))
        assertEquals(Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4)), filterSamples(4))
      }
    }
  }
  
  @Test
  def no_bad_values = { // t -> v
    val sample1 = Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2))
    val sample3 = Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5))
    val sample5 = Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 3.9))
    val sample6 = Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4))
    val sample7 = Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 3.1))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    val ds2 = MaxDeltaFilter("v", 1.0)(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        val filterSamples = it.toSeq
          
        assertEquals(Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5)), filterSamples(0))
        assertEquals(Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2)), filterSamples(1))
        assertEquals(Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6)), filterSamples(2))
        assertEquals(Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5)), filterSamples(3))
        assertEquals(Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 3.9)), filterSamples(4))
        assertEquals(Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4)), filterSamples(5))
        assertEquals(Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 3.1)), filterSamples(6))
      }
    }
  }
  
  @Test
  def two_dimensional_range = { // t -> (v, x)
    val sample1 = Sample(Integer(Metadata("t"), 1), Tuple(Real(Metadata("v"), 3.5), Real(Metadata("x"), 6.5)))
    val sample2 = Sample(Integer(Metadata("t"), 2), Tuple(Real(Metadata("v"), 3.2), Real(Metadata("x"), 6.2)))
    val sample3 = Sample(Integer(Metadata("t"), 3), Tuple(Real(Metadata("v"), 3.6), Real(Metadata("x"), 6.6)))
    val sample4 = Sample(Integer(Metadata("t"), 4), Tuple(Real(Metadata("v"), 3.5), Real(Metadata("x"), 6.5)))
    val sample5 = Sample(Integer(Metadata("t"), 5), Tuple(Real(Metadata("v"), 8.9), Real(Metadata("x"), 16.9)))
    val sample6 = Sample(Integer(Metadata("t"), 6), Tuple(Real(Metadata("v"), 3.4), Real(Metadata("x"), 6.4)))
    val sample7 = Sample(Integer(Metadata("t"), 7), Tuple(Real(Metadata("v"), 4.7), Real(Metadata("x"), 6.1)))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    val ds2 = MaxDeltaFilter("v", 2.0)(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        val filterSamples = it.toSeq
          
        assertEquals(Sample(Integer(Metadata("t"), 1), Tuple(Real(Metadata("v"), 3.5), Real(Metadata("x"), 6.5))), filterSamples(0))
        assertEquals(Sample(Integer(Metadata("t"), 2), Tuple(Real(Metadata("v"), 3.2), Real(Metadata("x"), 6.2))), filterSamples(1))
        assertEquals(Sample(Integer(Metadata("t"), 3), Tuple(Real(Metadata("v"), 3.6), Real(Metadata("x"), 6.6))), filterSamples(2))
        assertEquals(Sample(Integer(Metadata("t"), 4), Tuple(Real(Metadata("v"), 3.5), Real(Metadata("x"), 6.5))), filterSamples(3))
        assertEquals(Sample(Integer(Metadata("t"), 6), Tuple(Real(Metadata("v"), 3.4), Real(Metadata("x"), 6.4))), filterSamples(4))
        assertEquals(Sample(Integer(Metadata("t"), 7), Tuple(Real(Metadata("v"), 4.7), Real(Metadata("x"), 6.1))), filterSamples(5))
      }
    }
  }
  
  @Test
  def two_dimensional_domain = { // (lat, lon) -> v
    val sample1 = Sample(Tuple(Real(Metadata("lat"), 1.0), Real(Metadata("lon"), 1.0)), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Tuple(Real(Metadata("lat"), 2.0), Real(Metadata("lon"), 2.0)), Real(Metadata("v"), 3.2))
    val sample3 = Sample(Tuple(Real(Metadata("lat"), 3.0), Real(Metadata("lon"), 3.0)), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Tuple(Real(Metadata("lat"), 4.0), Real(Metadata("lon"), 4.0)), Real(Metadata("v"), 3.5))
    val sample5 = Sample(Tuple(Real(Metadata("lat"), 5.0), Real(Metadata("lon"), 5.0)), Real(Metadata("v"), 3.1))
    val sample6 = Sample(Tuple(Real(Metadata("lat"), 6.0), Real(Metadata("lon"), 6.0)), Real(Metadata("v"), 3.4))
    val sample7 = Sample(Tuple(Real(Metadata("lat"), 7.0), Real(Metadata("lon"), 7.0)), Real(Metadata("v"), 8.9))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    val ds2 = MaxDeltaFilter("v", 1.0)(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        val filterSamples = it.toSeq
          
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 1.0), Real(Metadata("lon"), 1.0)), Real(Metadata("v"), 3.5)), filterSamples(0))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 2.0), Real(Metadata("lon"), 2.0)), Real(Metadata("v"), 3.2)), filterSamples(1))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 3.0), Real(Metadata("lon"), 3.0)), Real(Metadata("v"), 3.6)), filterSamples(2))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 4.0), Real(Metadata("lon"), 4.0)), Real(Metadata("v"), 3.5)), filterSamples(3))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 5.0), Real(Metadata("lon"), 5.0)), Real(Metadata("v"), 3.1)), filterSamples(4))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 6.0), Real(Metadata("lon"), 6.0)), Real(Metadata("v"), 3.4)), filterSamples(5))
      }
    }
  }
  
  @Test
  def domain_2D_range_3D = { // ((lat, lon) -> (v, x, txt))
    val sample1 = Sample(Tuple(Real(Metadata("lat"), 1.0), Real(Metadata("lon"), 1.0)), 
                    Tuple(Real(Metadata("v"), 3.5), Integer(Metadata("x"), 6), Text(Metadata("txt"), "a")))
    val sample2 = Sample(Tuple(Real(Metadata("lat"), 2.0), Real(Metadata("lon"), 2.0)), 
                    Tuple(Real(Metadata("v"), 3.2), Integer(Metadata("x"), 6), Text(Metadata("txt"), "b")))
    val sample3 = Sample(Tuple(Real(Metadata("lat"), 3.0), Real(Metadata("lon"), 3.0)), 
                    Tuple(Real(Metadata("v"), 3.6), Integer(Metadata("x"), 6), Text(Metadata("txt"), "c")))
    val sample4 = Sample(Tuple(Real(Metadata("lat"), 4.0), Real(Metadata("lon"), 4.0)), 
                    Tuple(Real(Metadata("v"), 3.5), Integer(Metadata("x"), 6), Text(Metadata("txt"), "d")))
    val sample5 = Sample(Tuple(Real(Metadata("lat"), 5.0), Real(Metadata("lon"), 5.0)), 
                    Tuple(Real(Metadata("v"), 8.9), Integer(Metadata("x"), 16), Text(Metadata("txt"), "e")))
    val sample6 = Sample(Tuple(Real(Metadata("lat"), 6.0), Real(Metadata("lon"), 6.0)), 
                    Tuple(Real(Metadata("v"), 9.3), Integer(Metadata("x"), 17), Text(Metadata("txt"), "f")))
    val sample7 = Sample(Tuple(Real(Metadata("lat"), 7.0), Real(Metadata("lon"), 7.0)), 
                    Tuple(Real(Metadata("v"), 4.7), Integer(Metadata("x"), 7), Text(Metadata("txt"), "g")))
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6, sample7)
    
    val ds1 = Dataset(Function(samples, Metadata("test_function")), Metadata("test_dataset"))
    val ds2 = MaxDeltaFilter("x", 2.0)(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        val filterSamples = it.toSeq
          
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 1.0), Real(Metadata("lon"), 1.0)), 
                      Tuple(Real(Metadata("v"), 3.5), Integer(Metadata("x"), 6), Text(Metadata("txt"), "a"))), filterSamples(0))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 2.0), Real(Metadata("lon"), 2.0)), 
                      Tuple(Real(Metadata("v"), 3.2), Integer(Metadata("x"), 6), Text(Metadata("txt"), "b"))), filterSamples(1))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 3.0), Real(Metadata("lon"), 3.0)), 
                      Tuple(Real(Metadata("v"), 3.6), Integer(Metadata("x"), 6), Text(Metadata("txt"), "c"))), filterSamples(2))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 4.0), Real(Metadata("lon"), 4.0)), 
                      Tuple(Real(Metadata("v"), 3.5), Integer(Metadata("x"), 6), Text(Metadata("txt"), "d"))), filterSamples(3))
        assertEquals(Sample(Tuple(Real(Metadata("lat"), 7.0), Real(Metadata("lon"), 7.0)), 
                      Tuple(Real(Metadata("v"), 4.7), Integer(Metadata("x"), 7), Text(Metadata("txt"), "g"))), filterSamples(4))
      }
    }
  }
  
  
  
}