package latis.data

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._

class EmptyDataTest {

  @Test
  def equals {
    val d1 = Data.empty
    val d2 = EmptyData
    assertEquals(d1, d2)
  }
  
  @Test
  def is_empty = assert(EmptyData.isEmpty)
  
  @Test
  def not_not_empty = assert(! EmptyData.notEmpty)
  
  @Test
  def zero_length = assertEquals(0, EmptyData.length)
  
  @Test
  def zero_record_size = assertEquals(0, EmptyData.recordSize)
  
  @Test
  def zero_size = assertEquals(0, EmptyData.size)
  
  @Test
  def empty_iterator = assert(EmptyData.iterator.toList.isEmpty)
  
  @Test
  def empty_byte_buffer = assertEquals(0, EmptyData.getByteBuffer.limit)
  
  @Test
  def get_double = assertEquals(None, EmptyData.getDouble)
  
  @Test
  def get_string = assertEquals(None, EmptyData.getString)
  
  //TODO: doubleValue, stringValue, errors

}