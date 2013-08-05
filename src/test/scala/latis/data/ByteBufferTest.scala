package latis.data

import latis.data.value._
import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.data.value.DoubleValue
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import java.nio.CharBuffer

class ByteBufferTest {
  //TODO: DataTest trait that defines tests that each Data impl must pass?
  
  def testData(data: Data, test: Data => Any, expected: Any) = assertEquals(expected, test(data))
  
//  val datas: ArrayBuffer[Data] = ???
//  val tests: ArrayBuffer[Data => Any] = ???
//  for (data <- datas; test <- tests) 

  //zero length
  val emptyData = Data(ByteBuffer.allocate(0))
  
  //one record of one double, explicitly rewound
  val doubleDatum = Data(ByteBuffer.allocate(8).putDouble(3.14).flip)
  
  //one record of one double, rewound by constructor
  val doubleDatumNotRewound = Data(ByteBuffer.allocate(8).putDouble(3.14))
  
  //one record of three doubles
  val doubleDataRecord = Data(ByteBuffer.allocate(24).putDouble(1.0).putDouble(2.0).putDouble(3.0))
  
  //three records of one double
  val doubleDatumRecords = Data(ByteBuffer.allocate(24).putDouble(1.0).putDouble(2.0).putDouble(3.0), 8)
  
  //two records of two doubles
  val doubleDataRecords = Data(ByteBuffer.allocate(32).putDouble(1.0).putDouble(2.0).putDouble(3.0).putDouble(4.0), 16)
  
  //two records of two doubles, last one incomplete, limit should be 24 instead of 32
  //TODO: val doubleDataRecordsIncomplete = Data(ByteBuffer.allocate(32).putDouble(1.0).putDouble(2.0).putDouble(3.0), 16)
  
  
  //one record of one char
  //val charDatum = Data(ByteBuffer.allocate(2).putChar('A'))
  val charDatum = Data(CharBuffer.wrap(Array('A')))
      
  //one record of one string
  //val stringDatum = Data("Hello".foldLeft(ByteBuffer.allocate(10))(_.putChar(_)))
  val stringDatum = Data(CharBuffer.wrap("Hello"))
  
  //one record of two strings //TODO: but no way to specify length of strings
  val stringDataRecord = Data(CharBuffer.wrap("HelloWorld"))
  
  //two records of one string
  val stringDataRecords = Data(CharBuffer.wrap("HelloWorld"), 10)
  

  @Test
  def byte_buffer {
    //just confirming we get out what we put in, not needed for all
    assertEquals(ByteBuffer.allocate(0), emptyData.getByteBuffer)
    assertEquals(ByteBuffer.allocate(8).putDouble(3.14).flip, doubleDatum.getByteBuffer)
    assertEquals(ByteBuffer.allocate(8).putDouble(3.14).flip, doubleDatumNotRewound.getByteBuffer)
  }
  
  @Test
  def double_value = {
    //TODO: exception assertEquals( , emptyData.doubleValue)
    assertEquals(3.14 , doubleDatum.doubleValue, 0.0)
    assertEquals(3.14 , doubleDatumNotRewound.doubleValue, 0.0)
    assertTrue(doubleDataRecord.doubleValue.isNaN)
    assertTrue(doubleDatumRecords.doubleValue.isNaN)
    assertTrue(doubleDataRecords.doubleValue.isNaN)
    assertTrue(charDatum.doubleValue.isNaN)
    assertTrue(stringDatum.doubleValue.isNaN)
    assertTrue(stringDataRecord.doubleValue.isNaN)
    assertTrue(stringDataRecords.doubleValue.isNaN)
  }
  
  @Test
  def string_value = {
    //TODO: still need to consider what is correct behavior, ""? need same semantics a NaN, "getX" tests should cover it
    //TODO: error assertEquals( , emptyData.stringValue)
    //note not the same as toString
    assertEquals("3.14" , doubleDatum.stringValue)
    assertEquals("3.14" , doubleDatumNotRewound.stringValue)
    assertEquals("" , doubleDataRecord.stringValue)
    assertEquals("" , doubleDatumRecords.stringValue)
    assertEquals("" , doubleDataRecords.stringValue)
    assertEquals("A" , charDatum.stringValue)
    assertEquals("Hello" , stringDatum.stringValue)
    assertEquals("" , stringDataRecord.stringValue) //TODO: no way to diff from single string without length
    assertEquals("" , stringDataRecords.stringValue)
  }
  
  @Test
  def get_double_value = {
    assertEquals(None, emptyData.getDouble)
    assertEquals(Some(3.14), doubleDatum.getDouble)
    assertEquals(Some(3.14), doubleDatumNotRewound.getDouble)
    assertTrue(doubleDataRecord.getDouble.get.isNaN)
    assertTrue(doubleDatumRecords.getDouble.get.isNaN)
    assertTrue(doubleDataRecords.getDouble.get.isNaN)
    assertTrue(charDatum.getDouble.get.isNaN)
    assertTrue(stringDatum.getDouble.get.isNaN)
    assertTrue(stringDataRecord.getDouble.get.isNaN)
    assertTrue(stringDataRecords.getDouble.get.isNaN)
  }
  
  @Test
  def get_string_value = {
    //TODO: expect None if not a string? compare to NaN above
    assertEquals(None, emptyData.getString)
    assertEquals(Some("3.14"), doubleDatum.getString)
    assertEquals(Some("3.14"), doubleDatumNotRewound.getString)
    assertEquals(Some(""), doubleDataRecord.getString)
    assertEquals(Some(""), doubleDatumRecords.getString)
    assertEquals(Some(""), doubleDataRecords.getString)
    assertEquals(Some("A"), charDatum.getString)
    assertEquals(Some("Hello"), stringDatum.getString)
    assertEquals(Some(""), stringDataRecord.getString)  //TODO: no way to diff from single string without length
    assertEquals(Some(""), stringDataRecords.getString)
  }
  
  @Test
  def not_empty = {
    assertFalse(emptyData.notEmpty)
    assertTrue(doubleDatum.notEmpty)
    assertTrue(doubleDatumNotRewound.notEmpty)
    assertTrue(doubleDataRecord.notEmpty)
    assertTrue(doubleDatumRecords.notEmpty)
    assertTrue(doubleDataRecords.notEmpty)
    assertTrue(charDatum.notEmpty)
    assertTrue(stringDatum.notEmpty)
    assertTrue(stringDataRecord.notEmpty)
    assertTrue(stringDataRecords.notEmpty)
  }
  
  @Test
  def not_is_empty = {
    assertTrue(emptyData.isEmpty)
    assertFalse(doubleDatum.isEmpty)
    assertFalse(doubleDatumNotRewound.isEmpty)
    assertFalse(doubleDataRecord.isEmpty)
    assertFalse(doubleDatumRecords.isEmpty)
    assertFalse(doubleDataRecords.isEmpty)
    assertFalse(charDatum.isEmpty)
    assertFalse(stringDatum.isEmpty)
    assertFalse(stringDataRecord.isEmpty)
    assertFalse(stringDataRecords.isEmpty)
  }
  
  @Test
  def length = {
    assertEquals(0, emptyData.length)
    assertEquals(1, doubleDatum.length)
    assertEquals(1, doubleDatumNotRewound.length)
    assertEquals(1, doubleDataRecord.length)
    assertEquals(3, doubleDatumRecords.length)
    assertEquals(2, doubleDataRecords.length)
    assertEquals(1, charDatum.length)
    assertEquals(1, stringDatum.length)
    assertEquals(1, stringDataRecord.length)
    assertEquals(2, stringDataRecords.length)
  }
  
  @Test
  def recordSize = {
    assertEquals(0, emptyData.recordSize)
    assertEquals(8, doubleDatum.recordSize)
    assertEquals(8, doubleDatumNotRewound.recordSize)
    assertEquals(24, doubleDataRecord.recordSize)
    assertEquals(8, doubleDatumRecords.recordSize)
    assertEquals(16, doubleDataRecords.recordSize)
    assertEquals(2, charDatum.recordSize)
    assertEquals(10, stringDatum.recordSize)
    assertEquals(20, stringDataRecord.recordSize)
    assertEquals(10, stringDataRecords.recordSize)
  }
  
  @Test
  def size = {
    assertEquals(0, emptyData.size)
    assertEquals(8, doubleDatum.size)
    assertEquals(8, doubleDatumNotRewound.size)
    assertEquals(24, doubleDataRecord.size)
    assertEquals(24, doubleDatumRecords.size)
    assertEquals(32, doubleDataRecords.size)
    assertEquals(2, charDatum.size)
    assertEquals(10, stringDatum.size)
    assertEquals(20, stringDataRecord.size)
    assertEquals(20, stringDataRecords.size)
  }
  
  @Test
  def equals = {
    //TODO: consider equality issues, e.g. record size assumptions
    assertTrue(emptyData.equals(EmptyData))
    assertTrue(doubleDatum.equals(Data(3.14)))
    assertTrue(doubleDatumNotRewound.equals(Data(3.14)))
    //assertTrue(doubleDataRecord.equals(Data(Seq(1.0, 2.0, 3.0)))) //TODO: but Data from Seq assumes one datum per record
    assertTrue(doubleDatumRecords.equals(Data(Seq(1.0, 2.0, 3.0))))
    //assertTrue(doubleDataRecords.equals()) //TODO: but Data from Seq assumes one datum per record
    assertTrue(charDatum.equals(Data("A")))
    assertTrue(stringDatum.equals(Data("Hello")))
    //assertTrue(stringDataRecord.equals())
    //assertTrue(stringDataRecords.equals()) //TODO: but Data from Seq assumes one datum per record
  }
  
  @Test
  def iterate = {
    assertEquals(List.empty, emptyData.iterator.toList)
    assertTrue(List(Data(3.14)) equals doubleDatum.iterator.toList)
    assertEquals(List(Data(3.14)), doubleDatumNotRewound.iterator.toList)
    assertEquals(List(Data(Seq(1.0, 2.0, 3.0))), doubleDataRecord.iterator.toList)
    assertEquals(List(Data(1.0), Data(2.0), Data(3.0)), doubleDatumRecords.iterator.toList)
    assertEquals(List(Data(Seq(1.0, 2.0)), Data(Seq(3.0, 4.0))), doubleDataRecords.iterator.toList)
    assertEquals(List(Data("A")), charDatum.iterator.toList)
    assertEquals(List(Data("Hello")), stringDatum.iterator.toList)
    //assertEquals(List(Data(Seq("Hello", "World"))), stringDataRecord.iterator.toList) //TODO: support construction from Seq of strings
    assertEquals(List(Data("Hello"), Data("World")), stringDataRecords.iterator.toList)
  }
  
  //def iterate_twice
}