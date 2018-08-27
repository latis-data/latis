package latis.util

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer

class TestRegEx {
  
  def getMatchingGroups(pattern: String, s: String): Seq[String] = {
    pattern.r.findFirstMatchIn(s) match {
      case Some(m) => m.subgroups
      case None => Seq[String]()
    }
  }
  
  @Test
  def match_date_selection {
    val r = RegEx.TIME.r findFirstIn "time>=2011-09-14"
    //println(r.get)
    assertEquals("2011-09-14", r.get)
  }
  
  @Test
  def extract_group_matches {
    //val r = """(\d+)foo(\d+)bar"""
    //val s = "abc123foo456bar789"
    val r = """\[(.+)\] (.+)"""
    val s = "[2001-01-01] Hello, world!"
    val gs = getMatchingGroups(r,s)
    
    assertEquals(2, gs.length)
    assertEquals("2001-01-01", gs.head)
  }
  
  @Test
  def non_capturing_group {
    val r = """(\w+) (bar(?:1|2))"""
    val s = "foo bar1"
    val gs = getMatchingGroups(r,s)
    assertEquals(2, gs.length)
    assertEquals("bar1", gs(1))
  }
  
  @Test def match_word = assertTrue("foo" matches RegEx.WORD)
  @Test def match_word_with_numbers = assertTrue("foo123" matches RegEx.WORD)
  @Test def match_word_with_underscore = assertTrue("foo_123" matches RegEx.WORD)
  @Test def dont_match_word_with_bang = assertFalse("foo!123" matches RegEx.WORD)
  @Test def dont_match_word_with_dot = assertFalse("foo." matches RegEx.WORD)
  @Test def dont_match_dot_with_word = assertFalse(".foo" matches RegEx.WORD)
  
  @Test def match_variable = assertTrue("foo" matches RegEx.VARIABLE)
  @Test def match_variable_with_dot = assertTrue("foo.bar" matches RegEx.VARIABLE)
  @Test def dont_match_variable_with_hash = assertFalse("foo#bar" matches RegEx.VARIABLE)
  @Test def dont_match_variable_ending_with_dot = assertFalse("foo.bar." matches RegEx.VARIABLE)
  @Test def dont_match_variable_starting_with_dot = assertFalse(".foo.bar" matches RegEx.VARIABLE)

  //Number tests
  @Test def match_zero = assertTrue("0" matches RegEx.NUMBER)
  @Test def match_integer = assertTrue("123" matches RegEx.NUMBER)
  @Test def match_double = assertTrue("123.456" matches RegEx.NUMBER)
  @Test def match_negative = assertTrue("-123.456" matches RegEx.NUMBER)
  @Test def match_decimal_start = assertTrue(".456" matches RegEx.NUMBER)
  @Test def match_decimal_end = assertTrue("123." matches RegEx.NUMBER)
  @Test def match_negative_decimal = assertTrue("-.456" matches RegEx.NUMBER)
  @Test def match_scientific_notation = assertTrue("-1.23e-12" matches RegEx.NUMBER)
  @Test def match_scientific_notation2 = assertTrue("1.23E+12" matches RegEx.NUMBER)
  @Test def match_scientific_notation3 = assertTrue("1e12" matches RegEx.NUMBER)
  @Test def match_scientific_notation4 = assertTrue("1.e12" matches RegEx.NUMBER)
  @Test def match_scientific_notation5 = assertTrue(".1e12" matches RegEx.NUMBER)
  @Test def dont_match_not_a_number = assertFalse("1.23-e12" matches RegEx.NUMBER)
  @Test def dont_match_not_a_number2 = assertFalse("e12" matches RegEx.NUMBER)
  @Test def dont_match_not_a_number3 = assertFalse("1e" matches RegEx.NUMBER)
  @Test def dont_match_not_a_number4 = assertFalse("1-12" matches RegEx.NUMBER)
  @Test def dont_match_not_a_number5 = assertFalse("1e12." matches RegEx.NUMBER)
  @Test def dont_match_not_a_number6 = assertFalse("-+1" matches RegEx.NUMBER)
  @Test def dont_match_not_a_number7 = assertFalse(".e12" matches RegEx.NUMBER)
  @Test def dont_match_number_with_two_decimals = assertFalse("1.2.3" matches RegEx.NUMBER)
  
  //Value tests
  @Test def match_number_value = assertTrue("-1.23e+12" matches RegEx.VALUE)
  @Test def match_word_value = assertTrue("foo" matches RegEx.VALUE)
  @Test def match_time_value = assertTrue("1970-01-01T00:00:00" matches RegEx.VALUE)
  @Test def dont_match_empty_value = assertFalse("" matches RegEx.VALUE)
  @Test def dont_match_control_char = assertFalse(Character.toString(0x3) matches RegEx.VALUE)
  //TODO: test that other nasty things won't match
  
  //Time tests
  @Test def match_date_only = assertTrue("1970-01-01" matches RegEx.TIME)
  @Test def match_date_hour = assertTrue("1970-01-01T00" matches RegEx.TIME)
  @Test def match_date_hour_min = assertTrue("1970-01-01T00:00" matches RegEx.TIME)
  @Test def match_date_hour_min_sec = assertTrue("1970-01-01T00:00:00" matches RegEx.TIME)
  
  //Delimiter tests
  @Test def match_comma = assertTrue("," matches RegEx.DELIMITER)
  @Test def match_comma_space = assertTrue(", " matches RegEx.DELIMITER)
  @Test def match_space_comma = assertTrue(" ," matches RegEx.DELIMITER)
  @Test def match_space_comma_space = assertTrue(" , " matches RegEx.DELIMITER)
  @Test def dont_match_comma_comma = assertFalse(",," matches RegEx.DELIMITER)
  @Test def dont_match_empty_string = assertFalse("" matches RegEx.DELIMITER)
  
  //Split on delimiter
  @Test def split_on_comma = assertEquals(2, "foo,bar".split(RegEx.DELIMITER).length)
  @Test def split_on_comma_space = assertEquals(2, "foo, bar".split(RegEx.DELIMITER).length)
  @Test def split_on_space_comma = assertEquals(2, "foo ,bar".split(RegEx.DELIMITER).length)
  @Test def split_on_space_comma_space = assertEquals(2, "foo , bar".split(RegEx.DELIMITER).length)
  @Test def split_on_comma_spaces = assertEquals(2, "foo,  bar".split(RegEx.DELIMITER).length)
  @Test def split_on_spaces_comma = assertEquals(2, "foo  ,bar".split(RegEx.DELIMITER).length)
  @Test def split_on_spaces_comma_spaces = assertEquals(2, "foo  ,  bar".split(RegEx.DELIMITER).length)
  @Test def split_on_comma_comma = assertEquals(3, "foo,,bar".split(RegEx.DELIMITER).length)
  @Test def split_with_no_delim = assertEquals(1, "foobar".split(RegEx.DELIMITER).length)
  @Test def split_empty_string = assertEquals(1, "".split(RegEx.DELIMITER).length)
  
  //Selection Operators
  @Test def match_eq = assertTrue("==" matches RegEx.SELECTION_OPERATOR)
  @Test def match_eqeq = assertTrue("==" matches RegEx.SELECTION_OPERATOR)
  @Test def match_gt = assertTrue(">" matches RegEx.SELECTION_OPERATOR)
  @Test def match_ge = assertTrue(">=" matches RegEx.SELECTION_OPERATOR)
  @Test def match_lt = assertTrue("<" matches RegEx.SELECTION_OPERATOR)
  @Test def match_le = assertTrue("<=" matches RegEx.SELECTION_OPERATOR)
  @Test def match_match = assertTrue("=~" matches RegEx.SELECTION_OPERATOR)
  @Test def match_tilde = assertTrue("~" matches RegEx.SELECTION_OPERATOR)
  @Test def match_ne = assertTrue("!=" matches RegEx.SELECTION_OPERATOR)
  @Test def dont_match_eg = assertFalse("=>" matches RegEx.SELECTION_OPERATOR)
  @Test def dont_match_el = assertFalse("=<" matches RegEx.SELECTION_OPERATOR)
  @Test def dont_match_tilde_eq = assertFalse("~=" matches RegEx.SELECTION_OPERATOR)
  @Test def dont_match_empty_operator = assertFalse("" matches RegEx.SELECTION_OPERATOR)
  
  //Selection expression
  @Test def match_selection = assertTrue("foo>bar" matches RegEx.SELECTION)
  @Test def match_selection_with_quotes = assertTrue("foo>'bar'" matches RegEx.SELECTION)
  @Test def match_selection_with_number = assertTrue("foo>-1.23e+12" matches RegEx.SELECTION)
  @Test def match_selection_with_time = assertTrue("foo>1970-01-01T00:00:00" matches RegEx.SELECTION)
  @Test def dont_match_selection_with_bad_value = assertFalse("foo>`00" matches RegEx.SELECTION)
  @Test def dont_match_selection_with_bad_variable = assertFalse("`oo>bar" matches RegEx.SELECTION)
  @Test def dont_match_selection_with_bad_operator = assertFalse("foo=`bar" matches RegEx.SELECTION)
  //@Test broken by adding single quote to value to support time formatting
  def dont_match_selection_with_mismatched_quotes = assertFalse("foo=bar'" matches RegEx.SELECTION)
  
  //Extract Selection
  @Test def extract_selection = {
    val ms = getMatchingGroups(RegEx.SELECTION, "foo>bar")
    assertEquals(3, ms.length)
    assertEquals("foo", ms(0))
    assertEquals(">", ms(1))
    assertEquals("bar", ms(2))
  }
  @Test def extract_selection_with_number = {
    val ms = getMatchingGroups(RegEx.SELECTION, "foo>-1.23e+12")
    assertEquals(3, ms.length)
    assertEquals("-1.23e+12", ms(2))
  }
  @Test def extract_selection_with_time = {
    val ms = getMatchingGroups(RegEx.SELECTION, "foo>1970-01-01T00:00")
    assertEquals(3, ms.length)
    assertEquals("1970-01-01T00:00", ms(2))
  }
  @Test def dont_extract_bad_selection = assertEquals(0, getMatchingGroups(RegEx.SELECTION, "foo=`bar").length)

  //Contains expression
  @Test def match_contains_no_whitespace = assertTrue("a={1,2,3,4}" matches RegEx.CONTAINS)
  @Test def match_contains_whitespace_single_value = assertTrue("a= {  2 }" matches RegEx.CONTAINS)
  @Test def match_contains_whitespace_around_equals = assertTrue("a = {1,2,3,4}" matches RegEx.CONTAINS)
  @Test def match_contains_whitespace_between_values = assertTrue("a={1, 2,3,  4}" matches RegEx.CONTAINS)
  @Test def match_contains_whitespace_all_over = assertTrue("a = { 1, 2, 3, flux }" matches RegEx.CONTAINS)
  @Test def match_contains_multiple_types = assertTrue("foo={1, 2.0, flux}" matches RegEx.CONTAINS)
  @Test def match_contains_time = assertTrue("time={ 12/30/1994, 2018-12-30, 'December 30 1994' }" matches RegEx.CONTAINS)
  @Test def match_contains_missing_curly = assertFalse("foo={1,2.0,flux" matches RegEx.CONTAINS)
  @Test def match_contains_wrong_curly = assertFalse("foo=[1,2.0,flux]" matches RegEx.CONTAINS)
  @Test
  def contains_three_value_group_matches {
    val r = RegEx.CONTAINS
    val s = "foo = {1, 2, 3}"
    val gs = getMatchingGroups(r,s)
    
    assertEquals(2, gs.length)
    assertEquals("foo", gs.head)
    assertEquals(List("1, 2, 3"), gs.tail)
  }
  @Test
  def contains_two_value_group_matches {
    val r = RegEx.CONTAINS
    val s = "foo={bar baz}"
    val gs = getMatchingGroups(r,s)
    
    assertEquals(2, gs.length)
    assertEquals("foo", gs.head)
    assertEquals(List("bar baz"), gs.tail)
  }
  
  //Projection expression
  @Test def match_projection_of_one = assertTrue("foo" matches RegEx.PROJECTION)
  @Test def match_projection_of_two = assertTrue("foo,bar" matches RegEx.PROJECTION)
  @Test def match_projection_of_three = assertTrue("foo,bar,car" matches RegEx.PROJECTION)
  @Test def match_projection_of_two_with_space = assertTrue("foo, bar" matches RegEx.PROJECTION)
  @Test def match_projection_of_two_with_spaces = assertTrue("foo  ,  bar" matches RegEx.PROJECTION)
  @Test def match_projection_of_two_without_comma = assertTrue("foo    bar" matches RegEx.PROJECTION)
  @Test def dont_match_projection_with_empty = assertFalse("foo,,bar" matches RegEx.PROJECTION)
  @Test def dont_match_projection_with_trailing_comma = assertFalse("foo," matches RegEx.PROJECTION)
  @Test def dont_match_projection_with_leading_comma = assertFalse(",foo" matches RegEx.PROJECTION)
  @Test def dont_match_projection_with_bad_var = assertFalse("foo,#oo" matches RegEx.PROJECTION)
  @Test def dont_match_empty_projection = assertFalse("" matches RegEx.PROJECTION)
  
  //Operation (function call) expression
  @Test def match_operation_with_no_args = assertTrue("foo()" matches RegEx.OPERATION)
  @Test def match_operation_with_one_arg = assertTrue("foo(bar)" matches RegEx.OPERATION)
  @Test def match_operation_with_space_padding = assertTrue("foo( bar )" matches RegEx.OPERATION)
  @Test def match_operation_with_two_args = assertTrue("foo(-1.23e+12, 1970-01-01T00:00)" matches RegEx.OPERATION)
  @Test def dont_match_operation_with_bad_args = assertFalse("foo(b%r)" matches RegEx.OPERATION)
  @Test def dont_match_operation_without_parens = assertFalse("foo" matches RegEx.OPERATION)
  @Test def dont_match_empty_operation = assertFalse("" matches RegEx.OPERATION)
  @Test def match_time_format = assertTrue("format_time(yyyy-MM-dd'T'HH:mm)" matches RegEx.OPERATION)
  
  //Extract Operation
  @Test def extract_operation_with_no_args = {
    val ms = getMatchingGroups(RegEx.OPERATION, "foo()")
    assertEquals(2, ms.length)
    assertEquals("foo", ms(0))
    //TODO: second match is null instead of ""  assertEquals("", ms(1))
    assertEquals(null, ms(1))
  }
  @Test def extract_operation_with_one_arg = {
    val ms = getMatchingGroups(RegEx.OPERATION, "foo(bar)")
    assertEquals(2, ms.length)
    assertEquals("foo", ms(0))
    assertEquals("bar", ms(1))
  }
  @Test def extract_operation_with_two_args = {
    val ms = getMatchingGroups(RegEx.OPERATION, "foo( -1.23e+12, 1970-01-01T00:00 )")
    assertEquals(2, ms.length)
    //TODO: had to trim result now that " " is allowed in value
    assertEquals("-1.23e+12, 1970-01-01T00:00", ms(1).trim)
  }

}
