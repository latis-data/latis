package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.LazyLogging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter

class TestScalar {
 
  @Test
  def construct_scalar_from_boolean_true = {
    Scalar(1==1) match {
      case Text(s) => assertEquals("true", s)
    }
  }
  
  @Test
  def construct_scalar_from_boolean_false = {
    Scalar(1!=1) match {
      case Text(s) => assertEquals("false", s)
    }
  }
  
}