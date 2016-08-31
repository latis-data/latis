package latis.data

import latis.data.value._
import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.data.value.DoubleValue
import latis.metadata.Metadata
import latis.ops.RoundWithPrecision
import latis.ops.RoundWithSigfigs

class TestDoubleValue {

  val data = DoubleValue(3.14)
  
  @Test
  def byte_buffer {
    val bb = data.getByteBuffer
    assertEquals(8, bb.limit)
    assertEquals(3.14, bb.getDouble, 0.0)
  }
  
  @Test
  def double_value = assertEquals(3.14, data.doubleValue, 0.0)
  
  @Test
  def not_empty = assertTrue(data.notEmpty)
  
  @Test
  def not_is_empty = assertTrue(!data.isEmpty)
  
  @Test
  def rounding {
    //AsciiWriter.write(TestDataset.function_of_scalar_with_rounding)
    val ds = TestDataset.function_of_scalar_with_rounding
    val ds2 = RoundWithPrecision("a", 2)(ds) 
    val ds3 = RoundWithSigfigs("b", 1)(ds2)

    ds3 match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Real(r), Integer(i)) => {
          assertEquals(r, -0.00, 0.0)
          assertEquals(i, 100, 0.0)
        }
      }
    }
  }
  
  @Test @Ignore //we are not getting the expected behavior in text output
  def rounding2 {
    val ds = Dataset(Tuple(Real(Metadata(Map("precision"->"0", "name"->"a")),-0.004),
                           Real(Metadata(Map("precision"->"2", "name"->"b")),1.001111),
                           Real(Metadata(Map("sigfigs"->"3", "name"->"c")),1.995)
                     ))
    //AsciiWriter.write(ds)
    val data = ds.toStringMap
    assertEquals("-0", data("a").head)    //d=-0.004;   println(f"$d%.0f") => -0  //TODO: drop "-"?
    assertEquals("1.00", data("b").head)  //d=1.001111; println(f"$d%.2f") => 1.00
    assertEquals("2.00", data("c").head)  //d=1.995;    println(f"$d%.3g") => 2.00
  }
  
//  @Test
//  def length = assertEquals(1, data.length)
//  
//  @Test
//  def recordSize = assertEquals(8, data.recordSize)
  
  @Test
  def size = assertEquals(8, data.size)
  
  @Test
  def equals = assertEquals(data, Data(3.14))
  
//  @Test
//  def iterate = {
//    assertEquals(List(3.14), data.iterator.map(_.doubleValue).toList)
//  }
//  
//  @Test
//  def index = {
//    assertEquals(data, data(0))
//  }
//  
//  @Test
//  def index_out_of_bounds = {
//    try{
//      data(1)
//      fail
//    } catch {
//      case e: Exception => assertTrue(true)
//    }
//  }
  //def iterate_twice
  
  @Test
  def nan_equality = {
    val n1 = Data(Double.NaN)
    val n2 = Data(Double.NaN)
    assertNotEquals(n1, n2)
  }
}
