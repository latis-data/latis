package latis.data

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._

class DoubleValueTest {

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
  def string_value = assertEquals("3.14", data.stringValue)
  
  @Test
  def get_double_value = assertEquals(Some(3.14), data.getDouble)
  
  @Test
  def get_string_value = assertEquals(Some("3.14"), data.getString)
  
  @Test
  def not_empty = assert(data.notEmpty)
  
  @Test
  def not_is_empty = assert(!data.isEmpty)
  
  @Test
  def length = assertEquals(1, data.length)
  
  @Test
  def recordSize = assertEquals(8, data.recordSize)
  
  @Test
  def size = assertEquals(8, data.size)
  
  @Test
  def equals = assertEquals(data, Data(3.14))
  
  @Test
  def iterate = {
    assertEquals(List(3.14), data.iterator.map(_.doubleValue).toList)
  }
  
  //def iterate_twice
}