package latis.writer

import latis.dm.Dataset
import latis.dm.Index
import latis.dm.Variable

/**
 * Assume 1D, non-nested Function for now.
 */
class CsvWriter extends TextWriter {
  //TODO: more complex datasets
  
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
  
  /**
   * Override to define csv mime type. This tends to cause a web browser
   * to want to invoke an application instead of displaying it in the browser.
   */
  override def mimeType: String = getProperty("mimeType", "text/csv")

}