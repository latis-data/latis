package latis.data

import latis.data.value._
import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.data.value.DoubleValue
import latis.data.seq.SeqData

class SeqDataTest {

  val doubleSeq = Data(Seq(1.0, 2.0, 3.0))
  
  @Test
  def byte_buffer {
    val bb = doubleSeq.getByteBuffer
    assertEquals(24, bb.limit)
    assertEquals(24, bb.capacity)
    assertEquals(1.0, bb.getDouble(0), 0.0)
    assertEquals(2.0, bb.getDouble(8), 0.0)
  }
  
  @Test
  def byte_buffer_as_double_buffer {
    val db = doubleSeq.getByteBuffer.asDoubleBuffer()
    assertEquals(3.0, db.get(2), 0.0)
  }
  
  @Test
  def not_empty = assertTrue(doubleSeq.notEmpty)
  
  @Test
  def not_is_empty = assertTrue(!doubleSeq.isEmpty)
  
  @Test
  def length = assertEquals(3, doubleSeq.length)
  
  @Test
  def recordSize = assertEquals(8, doubleSeq.recordSize)
  
  @Test
  def size = assertEquals(24, doubleSeq.size)
  
  @Test
  def equals = assertEquals(doubleSeq, Data(Seq(1.0, 2.0, 3.0)))
  
  @Test
  def iterate = {
    val expected = List(Data(1.0), Data(2.0), Data(3.0)).map(_.getByteBuffer)
    val result = doubleSeq.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  
  @Test
  def index = {
    assertEquals(Data(2.0), doubleSeq(1))
  }
  
  @Test
  def index_out_of_bounds = {
    try{
      doubleSeq(3)
      fail
    } catch {
      case e: Exception => assertTrue(true)
    }
  }
  
  //def iterate_twice
}