package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.LazyLogging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter

class TestReal {
  
  @Test
  def double_value = assertEquals(3.14, TestReal.real_from_double.doubleValue, 0.0)
  
  @Test
  def double_value_from_int = assertEquals(3, TestReal.real_from_int.doubleValue, 0.0)
  
  @Test
  def double_value_from_string = assertEquals(3.14, TestReal.real_from_string.doubleValue, 0.0)
  
  @Test
  def int_value_round_down = assertEquals(3, TestReal.pi.intValue)
  
  //@Test
  //TODO: scala's toInt always truncates, should Real.intValue round?
  //  ScalaNumericAnyConversions.toInt says: may involve rounding or truncation, so not specified
  //  Math.round rounds up
  def int_value_round_up = assertEquals(3, TestReal.e.intValue)
  
  //@Test
  //TODO: always round up or round to even?
  //  This is the default rounding mode used in IEEE 754 computing functions and operators.
  //  That's good enough for me. 
  def int_value_round_mid = assertEquals(1, TestReal.half.intValue)
  
  @Test
  def name = assertEquals("pi", TestReal.pi.getName)
  
  @Test
  def unknown_name = assertEquals("unknown", TestReal.anon.getName)
}

/**
 * This object provides various Real Variables to use for other tests.
 */
object TestReal {
  
  def real_from_double = Real(3.14)
  def real_from_int = Real(3)
  def real_from_string = Real("3.14")
  
  def   pi = Real(Metadata("pi"), 3.14)
  def    e = Real(Metadata("e"), 2.718)
  def half = Real(Metadata("half"), 0.5)
  def anon = Real(3.14)
  
  //def _123 = Real(List(1.0, 2.0, 3.0))
  //TODO: unlimited, Stream
  
}