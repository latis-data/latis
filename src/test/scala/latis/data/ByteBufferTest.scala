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
  val doubleDatum = Data(ByteBuffer.allocate(8).putDouble(3.14).rewind)
  
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
  val charDatum = Data(ByteBuffer.allocate(2).putChar('A'))
  //val charDatum = Data(CharBuffer.wrap(Array('A'))) //needs to be BB.asCB
      
  //one record of one string
  val stringDatum = Data("Hello".foldLeft(ByteBuffer.allocate(10))(_.putChar(_)))
  //val stringDatum = Data(ByteBuffer.allocate(10).asCharBuffer.put("Hello"))
  
  //one record of two strings //TODO: but no way to specify length of strings
  val stringDataRecord = Data("HelloWorld".foldLeft(ByteBuffer.allocate(20))(_.putChar(_)))
  
  //two records of one string
  val stringDataRecords = Data("HelloWorld".foldLeft(ByteBuffer.allocate(20))(_.putChar(_)), 10)
  

  @Test def empty_byte_buffer = {
    assertEquals(ByteBuffer.allocate(0), emptyData.getByteBuffer)
  }
  @Test def double_byte_buffer = {
    assertEquals(ByteBuffer.allocate(8).putDouble(3.14).rewind, doubleDatum.getByteBuffer)
  }
  @Test def double_byte_buffer_not_rewound = {
    assertEquals(ByteBuffer.allocate(8).putDouble(3.14).rewind, doubleDatumNotRewound.getByteBuffer)
  }
  @Test def string_byte_buffer = {
    assertEquals("Hello".foldLeft(ByteBuffer.allocate(10))(_.putChar(_)).rewind, stringDatum.getByteBuffer)
  }
  //TODO: buffer capacity > limit, flip vs rewind
  
  // Test Data.notEmpty
  @Test def empty_data_not_empty               = assertFalse(emptyData.notEmpty)
  @Test def double_datum_not_empty             = assertTrue(doubleDatum.notEmpty)
  @Test def double_datum_not_rewound_not_empty = assertTrue(doubleDatumNotRewound.notEmpty)
  @Test def double_data_record_not_empty       = assertTrue(doubleDataRecord.notEmpty)
  @Test def double_datum_records_not_empty     = assertTrue(doubleDatumRecords.notEmpty)
  @Test def double_data_records_not_empty      = assertTrue(doubleDataRecords.notEmpty)
  @Test def char_datum_not_empty               = assertTrue(charDatum.notEmpty)
  @Test def string_datum_not_empty             = assertTrue(stringDatum.notEmpty)
  @Test def string_data_record_not_empty       = assertTrue(stringDataRecord.notEmpty)
  @Test def string_data_records_not_empty      = assertTrue(stringDataRecords.notEmpty)
  
  // Test Data.isEmpty
  @Test def empty_data_is_empty               = assertTrue(emptyData.isEmpty)
  @Test def double_datum_is_empty             = assertFalse(doubleDatum.isEmpty)
  @Test def double_datum_not_rewound_is_empty = assertFalse(doubleDatumNotRewound.isEmpty)
  @Test def double_data_record_is_empty       = assertFalse(doubleDataRecord.isEmpty)
  @Test def double_datum_records_is_empty     = assertFalse(doubleDatumRecords.isEmpty)
  @Test def double_data_records_is_empty      = assertFalse(doubleDataRecords.isEmpty)
  @Test def char_datum_is_empty               = assertFalse(charDatum.isEmpty)
  @Test def string_datum_is_empty             = assertFalse(stringDatum.isEmpty)
  @Test def string_data_record_is_empty       = assertFalse(stringDataRecord.isEmpty)
  @Test def string_data_records_is_empty      = assertFalse(stringDataRecords.isEmpty)

  // Test Data.length
  @Test def empty_data_length               = assertEquals(0, emptyData.length)
  @Test def double_datum_length             = assertEquals(1, doubleDatum.length)
  @Test def double_datum_not_rewound_length = assertEquals(1, doubleDatumNotRewound.length)
  @Test def double_data_record_length       = assertEquals(1, doubleDataRecord.length)
  @Test def double_datum_records_length     = assertEquals(3, doubleDatumRecords.length)
  @Test def double_data_records_length      = assertEquals(2, doubleDataRecords.length)
  @Test def char_datum_length               = assertEquals(1, charDatum.length)
  @Test def string_datum_length             = assertEquals(1, stringDatum.length)
  @Test def string_data_record_length       = assertEquals(1, stringDataRecord.length)
  @Test def string_data_records_length      = assertEquals(2, stringDataRecords.length)
  
  // Test Data.recordSize
  @Test def empty_data_record_size               = assertEquals(0, emptyData.recordSize)
  @Test def double_datum_record_size             = assertEquals(8, doubleDatum.recordSize)
  @Test def double_datum_not_rewound_record_size = assertEquals(8, doubleDatumNotRewound.recordSize)
  @Test def double_data_record_record_size       = assertEquals(24, doubleDataRecord.recordSize)
  @Test def double_datum_records_record_size     = assertEquals(8, doubleDatumRecords.recordSize)
  @Test def double_data_records_record_size      = assertEquals(16, doubleDataRecords.recordSize)
  @Test def char_datum_record_size               = assertEquals(2, charDatum.recordSize)
  @Test def string_datum_record_size             = assertEquals(10, stringDatum.recordSize)
  @Test def string_data_record_record_size       = assertEquals(20, stringDataRecord.recordSize)
  @Test def string_data_records_record_size      = assertEquals(10, stringDataRecords.recordSize)
  
  // Test Data.size
  @Test def empty_data_size               = assertEquals(0, emptyData.size)
  @Test def double_datum_size             = assertEquals(8, doubleDatum.size)
  @Test def double_datum_not_rewound_size = assertEquals(8, doubleDatumNotRewound.size)
  @Test def double_data_record_size       = assertEquals(24, doubleDataRecord.size)
  @Test def double_datum_records_size     = assertEquals(24, doubleDatumRecords.size)
  @Test def double_data_records_size      = assertEquals(32, doubleDataRecords.size)
  @Test def char_datum_size               = assertEquals(2, charDatum.size)
  @Test def string_datum_size             = assertEquals(10, stringDatum.size)
  @Test def string_data_record_size       = assertEquals(20, stringDataRecord.size)
  @Test def string_data_records_size      = assertEquals(20, stringDataRecords.size)

  // Test Data.equals
  //TODO: consider equality issues, e.g. record size assumptions
  @Test def empty_data_equals               = assertTrue(emptyData.equals(EmptyData))
  @Test def double_datum_equals             = assertTrue(doubleDatum.equals(Data(3.14)))
  @Test def double_datum_not_rewound_equals = assertTrue(doubleDatumNotRewound.equals(Data(3.14)))
    //assertTrue(doubleDataRecord.equals(Data(Seq(1.0, 2.0, 3.0)))) //TODO: but Data from Seq assumes one datum per record, need support for Tuple data?
  @Test def double_datum_records_equals     = assertTrue(doubleDatumRecords.equals(Data(Seq(1.0, 2.0, 3.0))))
    //assertTrue(doubleDataRecords.equals()) //TODO: but Data from Seq assumes one datum per record
  @Test def char_datum_equals               = assertTrue(charDatum.equals(Data("A")))
  @Test def string_datum_equals             = assertTrue(stringDatum.equals(Data("Hello")))
  //@Test def string_data_records_equals      = assertTrue(stringDataRecord.equals()) //TODO: support TextSeqData
    //assertTrue(stringDataRecords.equals()) //TODO: but Data from Seq assumes one datum per record
  
  
  // Test Data.iterator
  @Test def empty_data_iterate               = assertEquals(List.empty, emptyData.iterator.toList)
  @Test def double_datum_iterate = {
    val expected = List(Data(3.14).getByteBuffer)
    val result = doubleDatum.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  @Test def double_datum_not_rewound_iterate = {
    val expected = List(Data(3.14).getByteBuffer)
    val result = doubleDatumNotRewound.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  @Test def double_data_record_iterate = {
    val expected = List(Data(Seq(1.0, 2.0, 3.0)).getByteBuffer)
    val result = doubleDataRecord.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  @Test def double_datum_records_iterate = {
    val expected = List(Data(1.0), Data(2.0), Data(3.0)).map(_.getByteBuffer)
    val result = doubleDatumRecords.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  @Test def double_data_records_iterate = {
    val expected = List(Data(Seq(1.0, 2.0)), Data(Seq(3.0, 4.0))).map(_.getByteBuffer) //TODO: SeqData not rewound?
    val result = doubleDataRecords.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  @Test def char_datum_iterate = {
    val expected = List(Data("A").getByteBuffer)
    val result = charDatum.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  @Test def string_datum_iterate = {
    val expected = List(Data("Hello").getByteBuffer)
    val result = stringDatum.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  //@Test def string_data_record_iterate       = assertEquals(List(Data(Seq("Hello", "World"))), stringDataRecord.iterator.toList) //TODO: support construction from Seq of strings
  @Test def string_data_records_iterate = {
    val expected = List(Data("Hello"), Data("World")).map(_.getByteBuffer)
    val result = stringDataRecords.iterator.map(_.getByteBuffer).toList
    assertEquals(expected, result)
  }
  
  //TODO: def iterate_twice
}