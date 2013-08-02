package latis.data

import latis.data.value._
import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._

class StringValueTest {

  val data = StringValue("hello")
  
  @Test
  def byte_buffer {
    val bb = data.getByteBuffer
    assertEquals(10, bb.limit)
    assertEquals("hello", bb.toString)
  }
  
  @Test
  def double_value = assert(data.doubleValue.isNaN())
  //TODO: StringValue("3.14")?
  
  @Test
  def string_value = assertEquals("hello", data.stringValue)
  
  @Test
  def get_double_value = assert(data.getDouble.get.isNaN)
  
  @Test
  def get_string_value = assertEquals(Some("hello"), data.getString)
  
  @Test
  def not_empty = assert(data.notEmpty)
  
  @Test
  def not_is_empty = assert(!data.isEmpty)
  
  @Test
  def length = assertEquals(1, data.length)
  
  @Test
  def recordSize = assertEquals(10, data.recordSize)
  
  @Test
  def size = assertEquals(10, data.size)
  
  @Test
  def equals = assertEquals(data, Data("hello"))
  
  @Test
  def iterate = {
    assertEquals(List("hello"), data.iterator.map(_.stringValue).toList)
  }
  
  //def iterate_twice
}