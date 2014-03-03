package latis.util

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer

class TestRegEx {
  
  def getMatches(pattern: String, s: String): Seq[String] = {
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
    val vs = getMatches(r,s)
    
    assertEquals("2001-01-01", vs.head)
  }

}