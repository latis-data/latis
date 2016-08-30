package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.implicits._
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.TestDataset
import latis.dm.Test2D
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.ops.filter.Stride2D
import latis.time.Time
import latis.writer.AsciiWriter
import latis.data.Data
import latis.data.SampledData
import latis.data.seq.DataSeq

class TestStride2D {
  
  @Test
  def test_2d_with_stride_1_1_on_3x3 {
    assertEquals(Test2D.scalar_function_3x3, Stride2D(1, 1)(Test2D.scalar_function_3x3))
  }

  @Test
  def test_2d_with_stride_3_2_on_3x3 {
    val x = Integer(Metadata("X"))
    val y = Integer(Metadata("Y"))
    val a = Integer(Metadata("A"))
    
    val domainData = DataSeq(for (x <- 0 until 1; y <- 0 until 3 if y % 2 == 0) yield Data(x) concat Data(y))
    val rangeData = DataSeq(Seq.tabulate(1, 2)((x,y) => Data((x+y)*2)).flatten)
    val data = SampledData(domainData, rangeData)

    val correctDataset = Dataset(Function(Tuple(x,y), a, Metadata(Map("length"->"2")), data = data))
    
//    AsciiWriter.write(correctDataset)
//    AsciiWriter.write(Test2D.scalar_function_3x3)
   
    assertEquals(correctDataset, Stride2D(3, 2)(Test2D.scalar_function_3x3))
  }
  
  @Test
  def test_2d_with_stride_2_1_on_3x3 {
    val x = Integer(Metadata("X"))
    val y = Integer(Metadata("Y"))
    val a = Integer(Metadata("A"))
    
    val domainData = DataSeq(for (x <- 0 until 3 if x % 2 == 0; y <- 0 until 3) yield Data(x) concat Data(y))
    val rangeData = DataSeq(Seq.tabulate(2, 3)((x,y) => Data((x*2)+y)).flatten)
    val data = SampledData(domainData, rangeData)

    val correctDataset = Dataset(Function(Tuple(x,y), a, Metadata(Map("length"->"6")), data = data))
    
//    AsciiWriter.write(correctDataset)
//    AsciiWriter.write(Test2D.scalar_function_3x3)
   
    assertEquals(correctDataset, Stride2D(2, 1)(Test2D.scalar_function_3x3))
  }
  
  @Test
  def test_2d_with_stride_1_3_on_3x3 {
    val x = Integer(Metadata("X"))
    val y = Integer(Metadata("Y"))
    val a = Integer(Metadata("A"))
    
    val domainData = DataSeq(for (x <- 0 until 3; y <- 0 until 3 if y % 3 == 0) yield Data(x) concat Data(y))
    val rangeData = DataSeq(Seq.tabulate(3, 1)((x,y) => Data(x+y)).flatten)
    val data = SampledData(domainData, rangeData)

    val correctDataset = Dataset(Function(Tuple(x,y), a, Metadata(Map("length"->"3")), data = data))
    
//    AsciiWriter.write(correctDataset)
//    AsciiWriter.write(Test2D.scalar_function_3x3)
   
    assertEquals(correctDataset, Stride2D(1, 3)(Test2D.scalar_function_3x3))
  }
  
  @Test
  def test_2d_with_stride_3_1_on_4x3 {
    val x = Integer(Metadata("X"))
    val y = Integer(Metadata("Y"))
    val a = Integer(Metadata("A"))
    
    val domainData = DataSeq(for (x <- 0 until 4 if x % 3 == 0; y <- 0 until 3) yield Data(x) concat Data(y))
    val rangeData = DataSeq(Seq.tabulate(4, 3)((x,y) => Data(x+(y+(x*2)))).flatten)
    val data = SampledData(domainData, rangeData)

    val correctDataset = Dataset(Function(Tuple(x,y), a, Metadata(Map("length"->"6")), data = data))
    
//    AsciiWriter.write(correctDataset)
//    AsciiWriter.write(Test2D.scalar_function_4x3)
   
    assertEquals(correctDataset, Stride2D(3, 1)(Test2D.scalar_function_4x3))
  }
  
  @Test
  def test_2d_tuple_with_stride_2_2_on_2x4 {
    val x = Integer(Metadata("X"))
    val y = Integer(Metadata("Y"))
    val a = Integer(Metadata("A"))
    val b = Integer(Metadata("B"))
    
    val domainData = DataSeq(for (x <- 0 until 2 if x % 2 == 0; y <- 0 until 4 if y % 2 == 0) yield Data(x) concat Data(y))
    val rangeData = DataSeq(for (x <- 0 until 2 if x % 2 == 0; y <- 0 until 4 if y % 2 == 0) yield Data(x) concat Data(x+y))
    val data = SampledData(domainData, rangeData)
    
    val correctDataset = Dataset(Function(Tuple(x,y), Tuple(a, b), Metadata(Map("length"->"2")), data = data))
    
//    AsciiWriter.write(correctDataset)
//    AsciiWriter.write(Test2D.function_of_tuples_2x4)
   
    assertEquals(correctDataset, Stride2D(2, 2)(Test2D.function_of_tuples_2x4))
  }
  
  @Test
  def test_empty {
    assertEquals(Dataset.empty, Stride2D(3, 1)(Dataset.empty))
  }
  
  @Test
  def test_stride_0_0 {
    try {
      Stride2D(0, 0)(TestDataset.canonical)
      fail("stride(0, 0) should throw an exception")
    }
    catch {case e: Exception => }
  }
  
  @Test
  def test_stride_0_1 {
    try {
      Stride2D(0, 1)(TestDataset.canonical)
      fail("stride(0, x) should throw an exception")
    }
    catch {case e: Exception => }
  }
  
  @Test
  def test_stride_1_0 {
    try {
      Stride2D(1, 0)(TestDataset.canonical)
      fail("stride(x, 0) should throw an exception")
    }
    catch {case e: Exception => }
  }

}