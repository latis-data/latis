package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter
//import latis.time.Time
//import latis.time.TimeFormat
import java.util.Date

/**
 * Assume 1D, non-nested Function for now.
 */
class CsvWriter extends TextWriter {
  //TODO: Non-flat SSI,...
  
  override def makeHeader(dataset: Dataset): String = {
    //don't include Index variable
    dataset.toSeq.filterNot(_.isInstanceOf[Index]).map(makeHeading(_)).mkString("", delimiter, newLine) 
  }
  
  /**
   * Make a Variable heading for the header.
   */
  def makeHeading(variable: Variable): String = {
    val units = variable.getMetadata.get("units") match {
      case Some(u) => " (" + u + ")"
      case None => ""
    }
    variable.getName + units
  }
  
  override def mimeType: String = getProperty("mimeType", "text/csv")
  
  /*
   * TODO: CSV "standard":
   * http://tools.ietf.org/html/rfc4180
   * "there is no formal specification in existence"
   * tend not to have " " in delim
   */
}