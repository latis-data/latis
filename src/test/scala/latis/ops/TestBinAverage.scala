package latis.ops

import org.junit.Test
import org.junit.Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.writer.AsciiWriter

class TestBinAverage {
  
  //@Test //equality of NaNs is always false
  def bin1 {
    val op = new BinAverage(1.0)
    val expected = Dataset(Function(Seq(Real(0.0), Real(1), Real(2)), 
                                    Seq(Tuple(Real(0),Real(Metadata("min"),0),Real(Metadata("max"),0),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)), 
                                        Tuple(Real(1),Real(Metadata("min"),1),Real(Metadata("max"),1),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)), 
                                        Tuple(Real(2),Real(Metadata("min"),2),Real(Metadata("max"),2),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)))), Metadata("function_of_scalar"))
    assertEquals(expected, op(TestDataset.function_of_scalar))
  }
  
//  @Test 
  def bin2 {
    val op = new BinAverage(2.0)
    val expected = Dataset(Function(Seq(Real(0.5), Real(2)), 
                                    Seq(Tuple(Real(0.5),Real(Metadata("min"),0),Real(Metadata("max"),1),Real(Metadata("stddev"),Math.sqrt(2)),Integer(Metadata("count"),2)), 
                                        Tuple(Real(2),Real(Metadata("min"),2),Real(Metadata("max"),2),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)))), Metadata("function_of_scalar"))
    assertEquals(expected, op(TestDataset.function_of_scalar))
  }
  
  @Test 
  def bin3 {
    val op = new BinAverage(3.0)
    val expected = Dataset(Function(Seq(Real(1)), 
                                    Seq(Tuple(Real(1),Real(Metadata("min"),0),Real(Metadata("max"),2),Real(Metadata("stddev"),1),Integer(Metadata("count"),3)))), Metadata("function_of_scalar"))
    assertEquals(expected, op(TestDataset.function_of_scalar))
  }

}