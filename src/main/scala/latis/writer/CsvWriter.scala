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
  //TODO: DelimitedAsciiWriter?
  //TODO: Non-flat SSI,...
  
  def delimiter: String = ","
    //TODO: get from properties
    
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
  
  def makeScalar(scalar: Scalar): String = scalar match {
    //case _: Index => "" //don't include Index variable, deal with in Sample match
    case Real(d) => d.toString
    case Integer(l) => l.toString
    case Text(s) => s.trim
    case Binary(b) => "NaN" //TODO: uuencode?
    //TODO: use Scalar.toStringValue?
  //TODO: deal with Time format
  }
  
  def makeTuple(tuple: Tuple): String = tuple match {
    case Sample(d: Index, r) => varToString(r) //drop Index domain
    case Tuple(vars) => vars.map(varToString(_)).mkString(delimiter)
  }
  
  def makeFunction(function: Function): String = {
    function.iterator.map(varToString(_)).mkString(delimiter)
    // TODO: support non-flat, one row for each inner sample, repeat previous values
  }
  
  override def mimeType: String = "text/csv"
}