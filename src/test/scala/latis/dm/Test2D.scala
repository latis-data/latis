package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter
import latis.writer.Writer

class Test2D {
  
  //TODO: consider superclass for things like this
  def write(v: Variable) {
    AsciiWriter.write(Dataset(v))
  }
  
//  //@Test
//  def write_scalar_function_3x3 = write(Test2D.scalar_function_3x3)
//  
//  //@Test
//  def scalar_function_3x3_length {
//    val l = Test2D.scalar_function_3x3.getLength
//    assertEquals(9, l)
//  }
}

/**
 * Static two-dimensional variables to use for testing.
 */
object Test2D {
  
//  def scalar_function_3x3 = {
//    val x = Real(List(0.0, 1.0, 2.0))
//    val y = Real(List(0.0, 1.0, 2.0))
//    val a = Real((0 until 9).map(_.toDouble))
//    //TODO: use Seq.tabulate or fill to make Data
//    Function(Tuple(x,y), a)
//  }
  
}