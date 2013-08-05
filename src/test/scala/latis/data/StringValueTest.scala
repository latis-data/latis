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
    assertEquals("hello", bb.asCharBuffer.toString)
  }
  
  
  @Test
  def string_value = assertEquals("hello", data.stringValue)
  
  @Test
  def not_empty = assertTrue(data.notEmpty)
  
  @Test
  def not_is_empty = assertTrue(!data.isEmpty)
  
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