package latis.dm

import org.junit._
import Assert._
import latis.data.value.DoubleValue
import latis.writer._
import java.io.FileOutputStream
import java.nio.DoubleBuffer
import java.nio.ByteBuffer
import latis.data.value.StringValue
import latis.util.DataUtils
import latis.metadata.Metadata

class TestBinary {

  @Test
  def test_binary_double {
    val bytes = TestBinary.binary_double.getData.getBytes
    val d = ByteBuffer.wrap(bytes).getDouble
    assertEquals(3.14, d, 0.0)
  }

  @Test
  def test_binary_string {
    val bytes = TestBinary.binary_string match {case Binary(bs) => bs} //won't include termination marker
    val s = ByteBuffer.wrap(DataUtils.trimBytes(bytes)).asCharBuffer.toString
    assertEquals("Hello", s)
  }
  
  @Test
  def underfilled_buffer {
    val bb = ByteBuffer.allocate(20)
    bb.putDouble(3.14)
    bb.put(DataUtils.nullMark)
    val bv = Binary(bb)
    val ds = Dataset(bv)
    val bytes = ByteArrayWriter.getBytes(ds)
    assertEquals(8, bytes.length) //write only to the end of the data
    assertEquals(20, bv.getSize) //variable size is still full buffer capacity = limit + 8 byte marker
    val d = ByteBuffer.wrap(bytes).getDouble
    assertEquals(3.14, d, 0.0) //data value is preserved
  }
  
  @Test
  def no_termination_mark_with_length_metadata {
    val bb = ByteBuffer.allocate(8)
    bb.putDouble(3.14)
    val md = Metadata(Map("length" -> "8"))
    val bv = Binary(md, bb)
    val ds = Dataset(bv)
    val bytes = ByteArrayWriter.getBytes(ds)
    assertEquals(8, bytes.length) //write only to the end of the data
    assertEquals(16, bv.getSize) //variable size is still full buffer capacity = limit + 8 byte marker
    val d = ByteBuffer.wrap(bytes).getDouble
    assertEquals(3.14, d, 0.0) //data value is preserved
  }
  
  @Test
  def no_termination_mark {
    val bb = ByteBuffer.allocate(12)
    bb.putDouble(3.14)
    bb.putInt(42)
    val bv = Binary(bb)
    val ds = Dataset(bv)
    val bytes = ByteArrayWriter.getBytes(ds)
    assertEquals(12, bytes.length) //write only to the end of the data
    assertEquals(12, bv.getSize) //variable size is still full buffer capacity = limit + 8 byte marker
    val bb2 = ByteBuffer.wrap(bytes)
    assertEquals(3.14, bb2.getDouble, 0.0) //data value is preserved
    assertEquals(42, bb2.getInt, 0.0) //data value is preserved
  }
  
  
  /*
   * TODO: test overfilled buffer, but only the JdbcAdapter applies that now 
   * and configuring a database for that test would be a pain.
   * Consider factoring out the truncation logic into DataUtils? 
   * akin to StringUtils.padOrTruncate
   */
  
  //TODO: test time series of underfilled Binary with a selection
  //operations lead to a wrapped function that ends up using DataUtils.buildVarFromBuffer
  
}

/**
 * Static Binary variables to use for testing.
 */
object TestBinary {
  def binary_double = Binary(DataUtils.terminateBytes(DoubleValue(3.14).getByteBuffer))
  def binary_string = Binary(DataUtils.terminateBytes(StringValue("Hello").getByteBuffer))
}