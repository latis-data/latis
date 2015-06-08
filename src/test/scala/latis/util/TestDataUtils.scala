package latis.util

import org.junit._
import Assert._
import latis.dm._
import latis.metadata._
import latis.writer.AsciiWriter
import latis.data.seq.DataSeq
import latis.data.value.LongValue
import latis.data.value.DoubleValue
import latis.writer.Writer
import latis.ops.filter.FirstFilter
import latis.time.Time
import latis.data.value.StringValue

class TestDataUtils {

  @Test
  def test {
    val ds = Dataset(function_of_functions_of_tuple_from_data_map)
    //val ds2 = FirstFilter()(ds)
    val ds2 = FirstFilter()(ds)
    //AsciiWriter.write(ds)
    //Writer.fromSuffix("txt").write(ds)
    //Writer.fromSuffix("jsond").write(ds2)
    
    val data = ds2.toDoubleMap
    assertEquals(10.0, data("a")(0), 0.0)
  }

  @Test
  def real_double_value {
    val v = Real(3.14)
    assertEquals(3.14, DataUtils.getDoubleValue(v), 0.0)
  }

  @Test
  def integer_double_value {
    val v = Integer(3)
    assertEquals(3.0, DataUtils.getDoubleValue(v), 0.0)
  }

  @Test
  def text_time_double_value {
    val v = Time.fromIso("1970-01-01T00:00:01")
    assertEquals(1000.0, DataUtils.getDoubleValue(v), 0.0)
  }

  @Test
  def real_time_double_value = {
    val v = Time(1000.0)
    assertEquals(1000.0, DataUtils.getDoubleValue(v), 0.0)
  }
    
  @Test
  def buffer_to_string = {
    val s = "Hello"
    val bb = StringValue(s).getByteBuffer
    val s2 = DataUtils.bufferToString(bb, s.length)
    assertEquals(s, s2)
  }
  
  //-------------------------------------------------
  
  def function_of_scalar_from_data_map = {
    val domain = Integer(Metadata("x"))
    val range = Real(Metadata("a"))
        
    val dataMap = scala.collection.mutable.HashMap[String, DataSeq]()
    dataMap += ("x" -> DataSeq((0  until  3).map(LongValue(_))))
    dataMap += ("a" -> DataSeq((10 until 13).map(DoubleValue(_))))
    
    val data = DataUtils.dataMapToSampledData(dataMap, Sample(domain, range))
        
    Function(domain, range, Metadata("function_of_scalar_from_data_map"), data)
  }
    
  def function_of_tuple_from_data_map = {
    val domain = Integer(Metadata("x"))
    val range = Tuple(Real(Metadata("a")), Real(Metadata("b")))
        
    val dataMap = scala.collection.mutable.HashMap[String, DataSeq]()
    dataMap += ("x" -> DataSeq((0  until  3).map(LongValue(_))))
    dataMap += ("a" -> DataSeq((10 until 13).map(DoubleValue(_))))
    dataMap += ("b" -> DataSeq((20 until 23).map(DoubleValue(_))))
    
    val data = DataUtils.dataMapToSampledData(dataMap, Sample(domain, range))
        
    Function(domain, range, Metadata("function_of_tuple_from_data_map"), data)
  }
    
  def function_of_functions_of_scalar_from_data_map = {
    val domain = Integer(Metadata("x"))
    val range = Function(Integer(Metadata("y")), Real(Metadata("a")), Metadata(Map("length" -> "4")))
        
    val dataMap = scala.collection.mutable.HashMap[String, DataSeq]()
    dataMap += ("x" -> DataSeq((0  until  3).map(LongValue(_))))
    dataMap += ("y" -> DataSeq((0  until  4).map(LongValue(_))))
    dataMap += ("a" -> DataSeq((10 until 22).map(DoubleValue(_))))
    
    val data = DataUtils.dataMapToSampledData(dataMap, Sample(domain, range))
        
    Function(domain, range, Metadata("function_of_functions_of_scalar_from_data_map"), data)
  }
    
  def function_of_functions_of_tuple_from_data_map = {
    val domain = Integer(Metadata("x"))
    val range = Function(Integer(Metadata("y")), Tuple(Real(Metadata("a")), Real(Metadata("b"))), Metadata(Map("length" -> "4")))
        
    val dataMap = scala.collection.mutable.HashMap[String, DataSeq]()
    dataMap += ("x" -> DataSeq((0  until  3).map(LongValue(_))))
    dataMap += ("y" -> DataSeq((0  until  4).map(LongValue(_))))
    dataMap += ("a" -> DataSeq((10 until 22).map(DoubleValue(_))))
    dataMap += ("b" -> DataSeq((20 until 32).map(DoubleValue(_))))
    
    val data = DataUtils.dataMapToSampledData(dataMap, Sample(domain, range))
        
    Function(domain, range, Metadata("function_of_functions_of_tuple_from_data_map"), data)
  }
  
}