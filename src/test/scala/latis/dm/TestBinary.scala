package latis.dm

import org.junit._
import Assert._
import latis.data.value.DoubleValue
import latis.writer._
import java.io.FileOutputStream

class TestBinary {

}

/**
 * Static Binary variables to use for testing.
 */
object TestBinary {
  //TODO: add other instances
  def binary_double = Binary(DoubleValue(3.14).getByteBuffer)
}