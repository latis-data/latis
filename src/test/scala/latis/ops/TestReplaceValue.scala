package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.metadata.Metadata

class TestReplaceValue {

  @Test
  def test_no_match {
    val ds1 = Dataset(Real(Metadata("myReal"), 3.14))
    val ds2 = ds1.replaceValue(1.23, 6.28)
    assertEquals(3.14, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_text_with_double_values {
    val ds1 = Dataset(Text(Metadata("myText"), 3.14))
    val ds2 = ds1.replaceValue("3.14", "6.28")
    assertEquals("3.14", ds1.toStrings(0)(0)) //test immutability
    assertEquals("6.28", ds2.toStrings(0)(0))
  }
  
  @Test
  def test_real {
    val ds1 = Dataset(Real(Metadata("myReal"), 3.14))
    val ds2 = ds1.replaceValue(3.14, 6.28)
    assertEquals(3.14, ds1.toDoubles(0)(0), 0.0) //test immutability
    assertEquals(6.28, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_integer {
    val ds1 = Dataset(Integer(Metadata("myInt"), 3))
    val ds2 = ds1.replaceValue(3, 6)
    assertEquals(3, ds1.toDoubles(0)(0), 0.0)
    assertEquals(6, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def replace_real_with_string_value = {
    val ds1 = Dataset(Real(Metadata("myReal"), 3.14))
    val ds2 = ds1.replaceValue(3.14, "6.28")
    assertEquals(6.28, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def replace_real_as_string_with_string_value = {
    val ds1 = Dataset(Real(Metadata("myReal"), 3.14))
    val ds2 = ds1.replaceValue("3.14", "6.28")
    assertEquals(6.28, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def replace_int_as_string_with_string_value = {
    val ds1 = Dataset(Integer(Metadata("myInt"), 3))
    val ds2 = ds1.replaceValue("3", "6")
    assertEquals(6, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test //(expected=NumberFormatException.class)
  def replace_int_as_string_with_double_string_value = {
    val ds1 = Dataset(Integer(Metadata("myInt"), 3))
    try {
      val ds2 = ds1.replaceValue("3", "6.28")
      fail
    } catch {
      case nfe: NumberFormatException => assert(true)
      case e: Exception => fail //fail if we get any other kind of Exception
    }
  }

  @Test
  def test_nan {
    val ds1 = Dataset(Real(Metadata("myReal"), Double.NaN))
    val ds2 = ds1.replaceValue(Double.NaN, 6.28)

    assertEquals(6.28, ds2.toDoubles(0)(0), 0.0)
  }
  
  
  //TODO: test with Time
}




