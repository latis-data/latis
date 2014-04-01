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

  @Test def match_integer = assertTrue("123" matches RegEx.NUMBER)
  @Test def match_double = assertTrue("123.456" matches RegEx.NUMBER)
  @Test def match_negative = assertTrue("-123.456" matches RegEx.NUMBER)
  @Test def match_scientific_notation = assertTrue("-1.23e-12" matches RegEx.NUMBER)
  
  //@Test
  def match_selection {
    val s = "foo>bar"
  }

}