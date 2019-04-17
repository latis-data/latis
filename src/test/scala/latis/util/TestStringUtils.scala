package latis.util

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test

import latis.data.value.DoubleValue
import latis.data.value.LongValue
import latis.data.value.StringValue
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text
import latis.metadata.Metadata

class TestStringUtils {
  
  @Test
  def resolve_string_with_system_property {
    val s = StringUtils.resolveParameterizedString("Hi ${user.name}!")
    val a = "Hi " + System.getProperty("user.name") + "!"
    assertEquals(a, s)
  }
  
  @Test
  def resolve_string_with_environment_variable {
    val s = StringUtils.resolveParameterizedString("There's no place like ${HOME}.")
    val a = s"There's no place like ${System.getenv("HOME")}."
    assertEquals(a, s)
  }
  
  @Test
  def try_to_resolve_string_without_valid_property {
    //no changes made
    val s = StringUtils.resolveParameterizedString("Hi ${z1x2c3v4}!")
    val a = "Hi ${z1x2c3v4}!" 
    assertEquals(a, s)
  }
    
  @Test
  def resolve_string_with_latis_property {
    val s = StringUtils.resolveParameterizedString("Hi ${test.name}!")
    val a = "Hi George!"
    assertEquals(a, s)
  }
  
  @Test 
  def resolve_string_with_latis_property_with_space {
    val s = StringUtils.resolveParameterizedString("Hi ${test.name.with.space}!")
    val a = "Hi George !"
    assertEquals(a, s)
  }
  
  @Test
  def resolve_string_with_latis_property_without_new_line {
    val s = StringUtils.resolveParameterizedString("Hi ${test.name.without.nl}!")
    val a = "Hi George!"
    assertEquals(a, s)
  }
  
  @Test
  def pad_length {
    val s = StringUtils.padOrTruncate("abc", 5)
    val e = "abc  "
    assertEquals(e, s)
  }
  
  @Test
  def truncate_length {
    val s = StringUtils.padOrTruncate("abcd", 3)
    val e = "abc"
    assertEquals(e, s)
  }
  
  @Test
  def pad_var {
    val s = StringUtils.padOrTruncate("abc", Text(Metadata("length"->"5")))
    val e = "abc  "
    assertEquals(e, s)
  }
  
  @Test
  def trunc_var {
    val s = StringUtils.padOrTruncate("abcd", Text(Metadata("length"->"3")))
    val e = "abc"
    assertEquals(e, s)
  }
  
  @Test
  def numeric {
    assert(StringUtils.isNumeric("-1."))
    assert(StringUtils.isNumeric("5E-4"))
    assert(!StringUtils.isNumeric("two"))
  }
  
  @Test
  def toDouble {
    val d = StringUtils.toDouble("3e-4")
    val e = 3e-4
    assertEquals(e, d, 0.0)
  }
  
  @Test
  def toDouble_nan {
    val n = StringUtils.toDouble("one")
    assert(n.isNaN)
  }
  
  @Test
  def parse_int_int {
    val template = Integer(Metadata("int"))
    val d = StringUtils.parseStringValue("-1", template)
    d match {
      case LongValue(l) => assertEquals(-1, l)
      case _ => fail
    }
  }
  
  @Test
  def parse_int_double {
    val template = Integer(Metadata("int"))
    val d = StringUtils.parseStringValue("1.4", template)
    d match {
      case LongValue(l) => assertEquals(1, l)
      case _ => fail
    }
  }
  
  @Test
  def parse_int_exp {
    val template = Integer(Metadata("int"))
    val d = StringUtils.parseStringValue("1e3", template)
    d match {
      case LongValue(l) => assertEquals(1000, l)
      case _ => fail
    }
  }
  
  @Test
  def parse_int_fill {
    val template = Integer(Metadata("missing_value"->"12"))
    val d = StringUtils.parseStringValue("fill", template)
    d match {
      case LongValue(l) => assertEquals(12, l)
      case _ => fail
    }
  }
  
  @Test
  def parse_real_int {
    val template = Real(Metadata("real"))
    val d = StringUtils.parseStringValue("-1", template)
    d match {
      case DoubleValue(r) => assertEquals(-1.0, r, 0.0)
      case _ => fail
    }
  }
  
  @Test
  def parse_real_double {
    val template = Real(Metadata("real"))
    val d = StringUtils.parseStringValue("1.4", template)
    d match {
      case DoubleValue(r) => assertEquals(1.4, r, 0.0)
      case _ => fail
    }
  }
  
  @Test
  def parse_real_exp {
    val template = Real(Metadata("real"))
    val d = StringUtils.parseStringValue("1e3", template)
    d match {
      case DoubleValue(r) => assertEquals(1000.0, r, 0.0)
      case _ => fail
    }
  }
  
  @Test
  def parse_real_fill {
    val template = Real(Metadata("missing_value"->"3.14"))
    val d = StringUtils.parseStringValue("fill", template)
    d match {
      case DoubleValue(r) => assertEquals(3.14, r, 0.0)
      case _ => fail
    }
  }
  
  @Test
  def parse_text_pad {
    val template = Text(Metadata("length"->"5"))
    val d = StringUtils.parseStringValue("abc", template)
    d match {
      case StringValue(s) => assertEquals("abc  ", s)
      case _ => fail
    }
  }
  
  @Test
  def parse_text_trunc {
    val template = Text(Metadata("length"->"3"))
    val d = StringUtils.parseStringValue("abcd", template)
    d match {
      case StringValue(s) => assertEquals("abc", s)
      case _ => fail
    }
  }
  
  @Test
  def geturl_absolute {
    val e = "file:/foo bar/i"
    val s = StringUtils.getUrl(e).toString
    assertEquals(e, s)
  }
  
  @Test
  def geturl_file {
    val e = "file:/foo bar/i"
    val s = StringUtils.getUrl("/foo bar/i").toString
    assertEquals(e, s)
  }
  
  @Test
  def geturl_relative {
    val e = s"file:${scala.util.Properties.userDir}/foo bar/i"
    val s = StringUtils.getUrl("foo bar/i").toString
    assertEquals(e, s)
  }
}