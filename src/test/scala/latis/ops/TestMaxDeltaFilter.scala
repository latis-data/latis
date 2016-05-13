package latis.ops

import org.junit.Assert._
import org.junit.Test

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.metadata.Metadata
import latis.ops.filter.MaxDeltaFilter

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
  def three_bad_values = {
    val sample1 = Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 3.2))
    val sample3 = Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 12.4))
    val sample5 = Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 8.9))
    val sample6 = Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 9.6))
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
  def two_bad_values_apart = {
    val sample1 = Sample(Integer(Metadata("t"), 1), Real(Metadata("v"), 3.5))
    val sample2 = Sample(Integer(Metadata("t"), 2), Real(Metadata("v"), 9000.0))
    val sample3 = Sample(Integer(Metadata("t"), 3), Real(Metadata("v"), 3.6))
    val sample4 = Sample(Integer(Metadata("t"), 4), Real(Metadata("v"), 3.5))
    val sample5 = Sample(Integer(Metadata("t"), 5), Real(Metadata("v"), 3.9))
    val sample6 = Sample(Integer(Metadata("t"), 6), Real(Metadata("v"), 3.4))
    val sample7 = Sample(Integer(Metadata("t"), 7), Real(Metadata("v"), 16.1))
    
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
  def no_bad_values = {
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
  
  
  
}