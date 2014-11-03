package latis.ops

import org.junit.Test
import scala.math._
import org.junit.Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.writer.AsciiWriter
import latis.time.Time
import latis.reader.tsml.TsmlReader
import latis.server.DapConstraintParser
import scala.collection.mutable.ArrayBuffer
import latis.ops.math.MathOperation
import latis.ops.filter.Selection

class TestBinAverage {
  
  @Test 
  def bin1 {
    val op = new BinAverage(1.0)
    val expected = Dataset(Function(Seq(Real(0.0), Real(1), Real(2)), 
                                    Seq(Tuple(Real(0),Real(Metadata("min"),0),Real(Metadata("max"),0),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)), 
                                        Tuple(Real(1),Real(Metadata("min"),1),Real(Metadata("max"),1),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)), 
                                        Tuple(Real(2),Real(Metadata("min"),2),Real(Metadata("max"),2),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)))), Metadata("function_of_scalar"))
    assertEquals(expected, op(TestDataset.function_of_scalar))
  }
  
  @Test 
  def bin2 {
    val op = new BinAverage(2.0)
    val expected = Dataset(Function(Seq(Real(0.5), Real(2)), 
                                    Seq(Tuple(Real(0.5),Real(Metadata("min"),0),Real(Metadata("max"),1),Real(Metadata("stddev"),Math.sqrt(2)/2.0),Integer(Metadata("count"),2)), 
                                        Tuple(Real(2),Real(Metadata("min"),2),Real(Metadata("max"),2),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)))), Metadata("function_of_scalar"))
    assertEquals(expected, op(TestDataset.function_of_scalar))
  }
  
  @Test 
  def bin3 {
    val op = new BinAverage(3.0)
    val expected = Dataset(Function(Seq(Real(1)), Seq(Tuple(Real(1),Real(Metadata("min"),0),Real(Metadata("max"),2),Real(Metadata("stddev"),1),Integer(Metadata("count"),3)))), Metadata("function_of_scalar"))
    assertEquals(expected, op(TestDataset.function_of_scalar))
//    AsciiWriter.write(op(TestDataset.function_of_scalar))
  }
  
  @Test
  def time1 {
    val op = new BinAverage(86400000.0)
    val md = Metadata(Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd"))
    val expected = Dataset(Function(Seq(Time(md, 0.toLong), Time(md, 86400000.toLong), Time(md, 172800000.toLong)),
                                    Seq(Tuple(Real(Metadata("myReal"), 1.1),Real(Metadata("min"),1.1),Real(Metadata("max"),1.1),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)), 
                                        Tuple(Real(Metadata("myReal"), 2.2),Real(Metadata("min"),2.2),Real(Metadata("max"),2.2),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)), 
                                        Tuple(Real(Metadata("myReal"), 3.3),Real(Metadata("min"),3.3),Real(Metadata("max"),3.3),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)))), Metadata("time_series"))
    val ds = TestDataset.time_series
    //AsciiWriter.write(ds)
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
                                        
    assertEquals(expected, op(TestDataset.time_series))
  }
  
//  @Test
  def time2 {//subject to rounding errors
    val op = new BinAverage(86400000.0*2)
    val md = Metadata(Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd"))
    val expected = Dataset(Function(Seq(Time(md, 432000000.toLong), Time(md, 172800000.toLong)),
                                    Seq(Tuple(Real(Metadata("myReal"), 1.65),Real(Metadata("min"),1.1),Real(Metadata("max"),2.2),Real(Metadata("stddev"),sqrt((pow(1.1-1.65,2)+pow(2.2-1.65,2)))),Integer(Metadata("count"),2)), 
                                        Tuple(Real(Metadata("myReal"), 3.3),Real(Metadata("min"),3.3),Real(Metadata("max"),3.3),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)))), Metadata("time_series"))
    //AsciiWriter.write(op(TestDataset.time_series))
    assertEquals(expected, op(TestDataset.time_series))
  }
  
//  @Test
  def time3 {
    val op = new BinAverage(86400000.0*3)
    val md = Metadata(Map("name" -> "myTime", "type" -> "text", "length" -> "10", "units" -> "yyyy/MM/dd"))
    val expected = Dataset(Function(Seq(Time(md, 86400000.toLong)),
                                    Seq(Tuple(Real(Metadata("myReal"), 2.2),Real(Metadata("min"),1.1),Real(Metadata("max"),3.3),Real(Metadata("stddev"),1.1),Integer(Metadata("count"),3)))), Metadata("time_series"))
//    AsciiWriter.write(op(TestDataset.time_series))
    assertEquals(expected, op(TestDataset.time_series))
  }

  @Test
  def quikscat_telemetry_data {
    //val op = DapConstraintParser.parseExpression("binave(60000)")
    val ops = ArrayBuffer[Operation]()
    //ops += MathOperation((d: Double) => d*2)
    ops += Projection("time,myReal")
    ops += Selection("time>=2014-10-16T00:01")
    ops += Selection("time<2014-10-16T00:10")
    ops += new BinAverage(60000.0) //1 minute
    val ds = TsmlReader("binave.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    // ascii_iterative: (time -> (myReal, min, max, stddev, count))
    // 1413417689533 -> (21.631854255497455, 21.22629925608635, 21.65319925546646, 0.0938258653030056, 60)
    val data = ds.toDoubleMap
    assertEquals(1413417689533.0, data("time").head, 0.0)
    assertEquals(21.631854255497455, data("myReal").head, 0.0)
  }
}
