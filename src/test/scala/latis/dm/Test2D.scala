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
  
  @Test
  def scalar_function_3x3 {
    val ds = Dataset(Test2D.scalar_function_3x3)
    AsciiWriter.write(ds)
    //Writer("csv").write(ds)
  }
}

object Test2D {
  
  def scalar_function_3x3 = {
    val x = Real(List(0.0, 1.0, 2.0))
    val y = Real(List(0.0, 1.0, 2.0))
    val a = Real((0 until 9).map(_.toDouble))
    Function(Tuple(x,y), a)
  }
  
}