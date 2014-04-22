package latis.reader.tsml

import latis.util.RegEx
import latis.reader.tsml.ml.Tsml
import latis.data.Data
import latis.dm.Variable
import latis.data.value.IndexValue
import latis.dm._
import latis.util.StringUtils


class AsciiAdapter(tsml: Tsml) extends IterativeAdapter[String](tsml) {
  
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
  //TODO: reconcile with ability to define delimiter in tsml as regex, 
  //  but need to be able to insert into data
  
  /**
   * Return the number of lines (as returned by Source.getLines) that make up
   * each data record.
   */
  def getLinesPerRecord: Int = getProperty("linesPerRecord") match {
    case Some(s) => s.toInt
    case None => 1
  }
    
  /**
   * Return the number of lines (as returned by Source.getLines) that should
   * be skipped before reading data.
   */
  def getLinesToSkip: Int = getProperty("skip") match {
    case Some(s) => s.toInt
    case None => 0
  }
  
  /**
   * Return a list of variable names represented in the original data.
   * Note, this will not account Projections or other operations that
   * the adapter may apply.
   */
  def getVariableNames: Seq[String] = getOrigScalarNames

    
  //---- Parse operations -----------------------------------------------------
  
  /**
   * Return an Iterator of data records. Group multiple lines of text for each record.
   */
  def getRecordIterator: Iterator[String] = {
    val lpr = getLinesPerRecord
    val dlm = getDelimiter
    getLineIterator.grouped(lpr).map(_.mkString(dlm))
  }
  
  /**
   * Return Iterator of lines, filter out lines deemed unworthy by "shouldSkipLine".
   */
  def getLineIterator: Iterator[String] = {
    //TODO: does using 'drop' cause premature reading of data?
    val skip = getLinesToSkip
    getDataSource.getLines.drop(skip).filterNot(shouldSkipLine(_))
  }
  
  /**
   * This method will be used by the lineIterator to skip lines from the data source
   * that we don't want in the data. 
   * Note that the "isEmpty" test bypasses an end of file problem iterating over the 
   * iterator from Source.getLines.
   */
  def shouldSkipLine(line: String): Boolean = {
    //TODO: what if nothing but whitespace? trim?
    val c = getCommentCharacter
    line.isEmpty() || (c != null && line.startsWith(c))
  }
  
  /**
   * Return Map with Variable name to value(s) as Data.
   */
  def parseRecord(record: String): Option[Map[String,Data]] = {
    //TODO: consider factoring out extractValues(record) to get more reuse,
    //e.g. testing that we got the expected number of values
    
    /*
     * TODO: consider nested functions
     * if not flattened, lines per record will be length of inner Function (assume cartesian?)
     * deal with here or use algebra?
     */
    
    //assume one value per scalar per record
    val vars = getOrigScalars
    val values = extractValues(record)
    
    if (vars.length != values.length) None
    else {
      val vnames: Seq[String] = vars.map(_.getName)
      val datas: Seq[Data] = (values zip vars).map(p => parseStringValue(p._1, p._2))
      Some((vnames zip datas).toMap)
    }
  }
  
  /**
   * Extract the Variable values from the given record.
   */
  def extractValues(record: String): Seq[String] = record.split(getDelimiter)

  //TODO: reuse, dataUtils?
  //TODO: handle number parse exceptions
  def parseStringValue(value: String, variableTemplate: Variable): Data = variableTemplate match {
    case _: Index   => IndexValue(value.toInt)
    case _: Integer => Data(value.toLong)
    case _: Real    => Data(value.toDouble)
    case t: Text    => Data(StringUtils.padOrTruncate(value, t.length)) //enforce length
  }
}







