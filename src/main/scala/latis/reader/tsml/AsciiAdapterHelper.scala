package latis.reader.tsml

import latis.util.RegEx

/**
 * Trait for ASCII Adapters to use so we can reuse the AsciiParser
 * while keeping all Adapter logic out of it, yet support getting 
 * properties (e.g. delimiter, commentCharacter) from the TSML adapter 
 * definition.
 */
trait AsciiAdapterHelper extends AsciiParser { this: TsmlAdapter =>
  
  /**
   * Get the String (one or more characters) that is used at the start of a 
   * line to indicate that it should not be read as data. 
   * Defaults to null, meaning that no line should be ignored (except empty lines).
   * Return null if there are no comments to skip.
   */
  def getCommentCharacter: String = getProperty("commentCharacter") match {
    case Some(s) => s
    case None    => null
  }
  
  /**
   * Get the String (one or more characters) that is used to separate data values.
   * Default to comma (",").
   */
  def getDelimiter: String = getProperty("delimiter", ",")
  
  /**
   * Return the number of lines (as returned by Source.getLines) that make up
   * each data record.
   */
  def getLinesPerRecord: Int = getProperty("linesPerRecord") match {
    case Some(s) => s.toInt
    case None => 1
  }
    
//  /**
//   * Return the number of lines (as returned by Source.getLines) that should
//   * be skipped before reading data.
//   */
//  def getLinesToSkip: Int = getProperty("linesToSkip") match {
//    case Some(s) => s.toInt
//    case None => 0
//  }
  
  /**
   * Return a list of variable names represented in the original data.
   * Note, this will not account Projections or other operations that
   * the adapter may apply.
   */
  def getVariableNames: Seq[String] = origScalarNames

}