package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import scala.util.matching.Regex.Match

class FormattedAsciiAdapter(tsml: Tsml) extends RegexAdapter(tsml) {
  
  lazy val format = getProperty("format") match {
    case Some(s: String) => s
    case None => throw new RuntimeException("FormattedAsciiAdapter requires a format definition 'format'.")
  }
  
  override lazy val regex = formatToRegex(format)
  
  def formatToRegex(format: String) = {
    val int = (s: String) => """(\d*)I(\d+),?""".r.
      replaceAllIn(s, (m: Match) => {
        val length = m.group(2)
        val num = m.group(1) match {
          case "" => 1
          case s => s.toInt
        }
        s"(?:[ \\\\d]{$length}){$num}"
      })
    val float = (s: String) => """(\d*)F(\d+)\.(\d+),?""".r.
      replaceAllIn(s, (m: Match) => {
        val length = m.group(2)
        val num = m.group(1) match {
          case "" => 1
          case s => s.toInt
        }
        s"(?:[-. \\\\d]{$length}){$num}"
      })
    val nl = (s: String) => """\/""".r. 
      replaceAllIn(s, getDelimiter)
      
    int(float(nl(format))).r
      
  }
  
  override def extractValues(rec: String) = {
    super.extractValues(rec).map(_.replace(' ', '0'))
  }

}