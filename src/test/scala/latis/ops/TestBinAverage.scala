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
  def test_bin1 {
    val expected = Sample(Real(0.0),Tuple(Real(0),Real(Metadata("min"),0),Real(Metadata("max"),0),Real(Metadata("stddev"),Double.NaN),Integer(Metadata("count"),1)))
    BinAverage(1.0)(TestDataset.function_of_scalar) match {
      case Dataset(v) => assertEquals(expected, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  @Test 
  def test_bin1_length {
    assertEquals(3, BinAverage(1.0)(TestDataset.function_of_scalar).getLength)
  }
  
  @Test 
  def test_bin2 {
    val expected = Sample(Real(0.5),Tuple(Real(0.5),Real(Metadata("min"),0),Real(Metadata("max"),1),Real(Metadata("stddev"),Math.sqrt(2)/2.0),Integer(Metadata("count"),2)))
    BinAverage(2.0)(TestDataset.function_of_scalar) match {
      case Dataset(v) => assertEquals(expected, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  @Test
  def test_bin2_length{
    assertEquals(2, BinAverage(2.0)(TestDataset.function_of_scalar).getLength)
  }
  
  @Test 
  def test_bin3 {
    val expected = Sample(Real(1),Tuple(Real(1),Real(Metadata("min"),0),Real(Metadata("max"),2),Real(Metadata("stddev"),1),Integer(Metadata("count"),3)))
    BinAverage(3.0)(TestDataset.function_of_scalar) match {
      case Dataset(v) => assertEquals(expected, v.asInstanceOf[Function].iterator.next)
      case _ => fail()
    }
  }
  @Test
  def test_bin3_length {
    assertEquals(1, BinAverage(3.0)(TestDataset.function_of_scalar).getLength)
  }
  
  @Test
  def test_time1_length {
    assertEquals(3, BinAverage(86400000.0)(TestDataset.time_series).getLength)
  }
  
  @Test
  def test_time2_length {
    assertEquals(2, BinAverage(86400000.0*2)(TestDataset.time_series).getLength)
  }
  
  @Test
  def time3 {
    assertEquals(1, BinAverage(86400000.0*3)(TestDataset.time_series).getLength)
  }

  //-----BinAverageByWidth----------------------------------------------------------  
  
  @Test
  def quikscat_telemetry_data_by_width {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("time,myReal")
    ops += Selection("time>=2014-10-16T00:01")
    ops += Selection("time<2014-10-16T00:10")
    ops += new BinAverageByWidth(60000.0) //1 minute
    val ds = TsmlReader("binave.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    // ascii_iterative: (time -> (myReal, min, max, stddev, count))
    // 1413417689533 -> (21.631854255497455, 21.22629925608635, 21.65319925546646, 0.0938258653030056, 60)
    val data = ds.toDoubleMap
    
    assertEquals(9, data("time").length)
    assertEquals(1413417690033.0, data("time").head, 0.0)
    assertEquals(21.631854255497455, data("myReal").head, 0.0)
  }

  @Test
  def quikscat_telemetry_count_by_width {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("time,myReal,count")
    ops += Selection("time>=2014-10-16T00:01")
    ops += Selection("time<2014-10-16T00:10")
    ops += new BinAverageByWidth(60000.0) //1 minute
    val ds = TsmlReader("binave.tsml").getDataset(ops)
    val data = ds.toDoubleMap
    
    assertEquals(9, data("time").length)
    assertEquals(60, data("count").head, 0.0)
  }
  
  @Test
  def optional_param_test = {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("time,myReal,count")
    ops += Selection("time>=2014-10-16T00:01")
    ops += Selection("time<2014-10-16T00:10")
    ops += new BinAverageByWidth(60000.0, 1413417630000.0) //1 minute, starting at 2014-10-16 00:00:30
    ops += TimeFormatter("yyyy-MM-dd HH:mm:ss")
    val ds = TsmlReader("binave.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
     
    assertEquals(1413417660000.0, data("time").head, 0) 
    assertEquals(30, data("count").head, 0.0)
    assertEquals(60, data("count").tail.head, 0.0)
  }
  
  @Test
  def optional_param_before_beginning = {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("time,myReal,count")
    ops += Selection("time>=2014-10-16T00:01")
    ops += Selection("time<2014-10-16T00:10")
    ops += new BinAverageByWidth(30000.0, 1413416700000.0) //30 second bins, starting at 2014-10-15 23:45:00
    ops += TimeFormatter("yyyy-MM-dd HH:mm:ss")
    val ds = TsmlReader("binave.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
     
    assertEquals(1413416715000.0, data("time").head, 0) 
    assertEquals(0.0, data("count").head, 0.0)
  }
  
  @Test
  def optional_param_at_beginning = {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("time,myReal,count")
    ops += Selection("time>=2014-10-16T00:01")
    ops += Selection("time<2014-10-16T00:10")
    ops += new BinAverageByWidth(15000.0, 1413417660033.0) //15 second bins, starting at 2014-10-16 00:01:00.033
    ops += TimeFormatter("yyyy-MM-dd HH:mm:ss")
    val ds = TsmlReader("binave.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap

    assertEquals(1413417667000.0, data("time").head, 0) 
    assertEquals(15, data("count").head, 0.0)
    assertEquals(15, data("count").tail.head, 0.0)
  }
  
  @Test
  def optional_param_just_after_beginning = { //Assert that an exception is thrown
    var thrown = false
    try {
      val ops = ArrayBuffer[Operation]()
      ops += Projection("time,myReal,count")
      ops += Selection("time>=2014-10-16T00:01")
      ops += Selection("time<2014-10-16T00:10")
      ops += new BinAverageByWidth(30000.0, 1413417675000.0) //30 second bins, starting at 2014-10-16 00:01:15
      ops += TimeFormatter("yyyy-MM-dd HH:mm:ss")
      val ds = TsmlReader("binave.tsml").getDataset(ops)
      //AsciiWriter.write(ds)
      val data = ds.toDoubleMap
    } catch {
      case e: UnsupportedOperationException =>  thrown = true
    }
     
    assertEquals(true, thrown)
    //OLD TEST RESULTS BEFORE ERROR THROWING
    //assertEquals(1413417690000.0, data("time").head, 0) 
    //assertEquals(45, data("count").head, 0.0)
    //assertEquals(30, data("count").tail.head, 0.0)
  }
  
  @Test
  def optional_param_after_data = { //Assert that an exception is thrown
    var thrown = false
    try {
      val ops = ArrayBuffer[Operation]()
      ops += Projection("time,myReal,count")
      ops += Selection("time>=2014-10-16T00:01")
      ops += Selection("time<2014-10-16T00:10")
      ops += new BinAverageByWidth(60000.0, 1413418260000.0) //1 minute bins, starting at 2014-10-16 00:11:00
      ops += TimeFormatter("yyyy-MM-dd HH:mm:ss")
      val ds = TsmlReader("binave.tsml").getDataset(ops)
      //AsciiWriter.write(ds)
      val data = ds.toDoubleMap
    } catch {
      case e: UnsupportedOperationException =>  thrown = true
    }

    assertEquals(true, thrown)
    //OLD TEST RESULTS BEFORE ERROR THROWING
    //assertEquals(540, data("count").head, 0.0) //All data in one bin; not what we want
  }
  
  @Test
  def optional_param_middle_of_data = { //Assert that an exception is thrown
    var thrown = false
    try {
      val ops = ArrayBuffer[Operation]()
      ops += Projection("time,myReal,count")
      ops += Selection("time>=2014-10-16T00:01")
      ops += Selection("time<2014-10-16T00:10")
      ops += new BinAverageByWidth(15000.0, 1413417930000.0) //15 second bins, starting at 2014-10-16 00:05:30
      ops += TimeFormatter("yyyy-MM-dd HH:mm:ss")
      val ds = TsmlReader("binave.tsml").getDataset(ops)
      //AsciiWriter.write(ds)
      val data = ds.toDoubleMap
    } catch {
      case e: UnsupportedOperationException =>  thrown = true
    }

    assertEquals(true, thrown)
    //OLD TEST RESULTS BEFORE ERROR THROWING
    //assertEquals(1413417937000.0, data("time").head, 0.0)
    //assertEquals(285, data("count").head, 0.0) //285 is from previous data; we really don't want that data
    //assertEquals(15, data("count").tail.head, 0.0)
  }
  
  
  //CREATE TEST WITH EMPTY DATASET. Return empty DS, formatted as if bins existed? (A, A min, A max, Count)
  //(not yet implemented)
  @Test
  def optional_param_empty_ds = { 
    val ops = ArrayBuffer[Operation]()
    ops += Projection("time,myReal,count")
    ops += Selection("time=0")
    ops += new BinAverageByWidth(15000.0, 0.0) //15 second bins
    ops += TimeFormatter("yyyy-MM-dd HH:mm:ss")
    val ds = TsmlReader("binave.tsml").getDataset(ops)
    AsciiWriter.write(ds)
    
    //val data = ds.toDoubleMap
    //assertEquals(0, data("count").head, 0.0)
  }

  @Test
  def optional_param_test_zero = {
    assertEquals(3, BinAverageByWidth(86400000.0, 0.0)(TestDataset.time_series).getLength)
  }
  
  
//-----deprecated test--------------------------------------------------------------------------------------------------  
  
//  @Test
//  def quikscat_telemetry_data_by_count {
//    //val op = DapConstraintParser.parseExpression("binave(60000)")
//    val ops = ArrayBuffer[Operation]()
//    //ops += MathOperation((d: Double) => d*2)
//    ops += Projection("time,myReal")
//    ops += Selection("time>=2014-10-16T00:01")
//    ops += Selection("time<2014-10-16T00:10")
//    //ops += new BinAverageByCount(9) //9 bins
//    val ds = TsmlReader("binave.tsml").getDataset(ops)
//    //add time range metadata
//    val props = ds.getMetadata.getProperties + ("time_min" -> "2014-10-16T00:01") + ("time_max" -> "2014-10-16T00:10")
//    val md = Metadata(props)
//    val ds2 = Dataset(ds.getVariables, md)
//    val ds3 = BinAverageByCount(9)(ds2)
//    //AsciiWriter.write(ds3)
//    val data = ds3.toDoubleMap
//    assertEquals(9, data("time").length)
//    assertEquals(1413417690033.0, data("time").head, 0.0)
//    assertEquals(21.631854255497455, data("myReal").head, 0.0)
//  }
  
}
