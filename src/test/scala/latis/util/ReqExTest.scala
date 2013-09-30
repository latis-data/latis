package latis.util

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer

class RegExTest {
  //TODO: load special properties file, controlled environment
  
  def getMatches(pattern: String, s: String): Seq[String] = {
    pattern.r.findFirstMatchIn(s) match {
      case Some(m) => m.subgroups
      case None => Seq[String]()
    }
  }
  
  @Test
  def match_date_selection {
    val r = RegEx.TIME findFirstIn "time>=2011-09-14"
    //println(r.get)
    assertEquals("2011-09-14", r.get)
  }
  
  //@Test
  def extract_group_matches {
    //val r = """(\d+)foo(\d+)bar"""
    //val s = "abc123foo456bar789"
    val r = """\[(.+)\] (.+)"""
    val s = "[2001-01-01] Hello, world!"
    val vs = getMatches(r,s)
    
    println(vs.mkString(","))
  }
  
  @Test
  def parse_log_file {
    val ds = TsmlReader("datasets/test/log.tsml").getDataset
    Writer("csv").write(ds)
    /*
     * Text variable has data = StringSeqData
     * but data iterator consolidates data via a byte buffer
     * Text length never applied so buffer underflow
     */
  }
}