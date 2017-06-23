package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import scala.util.matching.Regex.Match
import scala.util.matching.Regex

class FormattedAsciiAdapter(tsml: Tsml) extends RegexAdapter(tsml) {
  
  lazy val format = getProperty("format") match {
    case Some(s: String) => s
    case None => throw new RuntimeException("FormattedAsciiAdapter requires a format definition 'format'.")
  }
  
  override lazy val regex: Regex = formatToRegex(format)
  
  def formatToRegex(format: String): Regex = {
    //eg: 3I2 => (?:[ \d]{2}){3}
    //matches "123456"
    val int = (s: String) => """(\d*)I(\d+),?""".r.
      replaceAllIn(s, (m: Match) => {
        val length = m.group(2)
        val num = m.group(1) match {
          case "" => 1
          case s => s.toInt
        }
        s"(?:[ \\\\d]{$length}){$num}"
      })
    //eg: 3F7.2 => (?:[ -\d]{4}\.[\d]{2}){3}
    //matches "1234.56-123.45 -12.34"
    val float = (s: String) => """(\d*)F(\d+)\.(\d+),?""".r.
      replaceAllIn(s, (m: Match) => {
        val length = m.group(2).toInt
        val decimal = m.group(3).toInt
        val num = m.group(1) match {
          case "" => 1
          case s => s.toInt
        }
        s"(?:[- \\\\d]{${length-decimal-1}}\\\\.[\\\\d]{$decimal}){$num}"
      })
    //eg: 2A3 => (?:.{3}){2}
    //matches "123ABC"
    val string = (s: String) => """(\d*)A(\d*),?""".r.
      replaceAllIn(s, (m: Match) => {
        val length = m.group(2) match {
          case "" => 1
          case s => s.toInt
        }
        val num = m.group(1) match {
          case "" => 1
          case s => s.toInt
        }
        s"(?:.{$length}){$num}"
      })
    //new lines are replaced by delimiters for linesPerRecord > 1,
    //so replace '/' in the format with a delimiter.
    val nl = (s: String) => """\/""".r. 
      replaceAllIn(s, getDelimiter)
      
    int(float(string(nl(format)))).r
      
  }
  
  override def extractValues(rec: String): Seq[String] = {
    super.extractValues(rec).map(_.replace(' ', '0'))
  }

}