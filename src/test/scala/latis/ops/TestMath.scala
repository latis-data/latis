package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import java.nio.ByteBuffer
import latis.data.buffer.ByteBufferData
import latis.metadata.Metadata

class TestMath  {
  
  @Test
  def real_addition {
    val pi = Real(3.14)
    val pi2 = pi + pi
    val r = pi2.unwrap.asInstanceOf[Real].doubleValue
    assertEquals(6.28, r, 0.0)
  }  
 
  @Test
  def real_multiplication {
    val pi = Real(3.14)
    val pi2 = pi * 2
    val r = pi2.unwrap.asInstanceOf[Real].doubleValue
    assertEquals(6.28, r, 0.0)
  }

  //@Test
//  def add_double_to_function {
//    val f = TestFunction.function_of_scalar_with_data_from_kids
//    //AsciiWriter.write(f)
//    val f2 = f + 1
//    //AsciiWriter.write(f2)
//    val dd = f2.toDoubles //TODO: breaks without named scalars
//    assertEquals(1.0, dd(1)(0), 0.0)
//  }
}