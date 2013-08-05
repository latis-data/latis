package latis.data

import latis.data.value._
import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.data.value.DoubleValue

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
  def not_empty = assertTrue(data.notEmpty)
  
  @Test
  def not_is_empty = assertTrue(!data.isEmpty)
  
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