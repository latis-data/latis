package latis.dm

import org.junit._
import Assert._
import latis.data.value.DoubleValue
import latis.writer._
import java.io.FileOutputStream
import java.nio.DoubleBuffer
import java.nio.ByteBuffer
import latis.data.value.StringValue

class TestBinary {

  @Test
  def test_binary_double {
    val bytes = TestBinary.binary_double.getData.getBytes
    val d = ByteBuffer.wrap(bytes).getDouble
    assertEquals(3.14, d, 0.0)
  }

  @Test
  def test_binary_string {
    val bytes = TestBinary.binary_string.getData.getBytes
    val s = ByteBuffer.wrap(bytes).asCharBuffer.toString
    assertEquals("Hello", s)
  }
  
  @Test
  def underfilled_buffer {
    val bb = ByteBuffer.allocate(10)
    bb.putDouble(3.14)
    val bv = Binary(bb)
    val ds = Dataset(bv)
    val bytes = ByteArrayWriter.getBytes(ds)
    assertEquals(8, bytes.length) //write only to the end of the data
    assertEquals(10, bv.getSize) //variable size is still full buffer capacity
    val d = ByteBuffer.wrap(bytes).getDouble
    assertEquals(3.14, d, 0.0) //data value is preserved
  }
  
  /*
   * TODO: test overfilled buffer, but only the JdbcAdapter applies that now 
   * and configuring a database for that test would be a pain.
   * Consider factoring out the truncation logic into DataUtils? 
   * akin to StringUtils.padOrTruncate
   */
  
}

/**
 * Static Binary variables to use for testing.
 */
object TestBinary {
  def binary_double = Binary(DoubleValue(3.14).getByteBuffer)
  def binary_string = Binary(StringValue("Hello").getByteBuffer)
}