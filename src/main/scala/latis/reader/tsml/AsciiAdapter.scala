package latis.reader.tsml

import latis.data.Data
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text
import latis.dm.Variable
import latis.reader.tsml.ml.Tsml
import latis.util.StringUtils
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging


class AsciiAdapter(tsml: Tsml) extends IterativeAdapter2[String](tsml) with Logging {

  //---- Manage data source ---------------------------------------------------
  
  private var source: Source = null
  
  /**
   * Get the Source from which we will read data.
   */
  def getDataSource: Source = {
    if (source == null) source = Source.fromURL(getUrl)
    source
  }
  
  override def close {
    if (source != null) source.close
  }
  
  //---- Adapter Properties ---------------------------------------------------
  
  /**
   * Get the String (one or more characters) that is used at the start of a 
   * line to indicate that it should not be read as data. 
   * Defaults to null, meaning that no line should be ignored (except empty lines).
   * Return null if there are no comments to skip.
   * Use a lazy val since this will be used for every line.
   */
  lazy val getCommentCharacter: String = getProperty("commentCharacter") match {
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

  /**
   * Get the String used as the data marker from tsml file.
   * Use a lazy val since this will be used for every line.
   */
  lazy val getDataMarker: String = getProperty("marker") match {
    case Some(s) => s
    case None => null
  }
  
  /**
   * Keep track of whether we have encountered a data marker.
   */
  private var foundDataMarker = false


  //---- Parse operations -----------------------------------------------------
  
  /**
   * Return an Iterator of data records. Group multiple lines of text for each record.
   */
  def getRecordIterator: Iterator[String] = {
    val lpr = getLinesPerRecord
    val dlm = getDelimiter
    val records = getLineIterator.grouped(lpr).map(_.mkString(dlm))
    
    getProperty("limit") match {
      case Some(s) => records.take(s.toInt) //TODO: deal with bad value
      case None    => records
    }
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
    val d = getDataMarker
    val c = getCommentCharacter

    if (d == null || foundDataMarker) {
      // default behavior: ignore empty lines and lines that start with comment characters
      line.isEmpty() || (c != null && line.startsWith(c))
    } else {
      // We have a data marker and we haven't found it yet,
      // therefore we should ignore everything until we
      // find it. We should also exclude the data marker itself
      // when we find it. 
      if (line.startsWith(d)) foundDataMarker = true;
      true
    }
  }
  
  /**
   * Return Map with Variable name to value(s) as Data.
   */
  def parseRecord(record: String): Option[Map[String,Data]] = {
    /*
     * TODO: consider nested functions
     * if not flattened, lines per record will be length of inner Function (assume cartesian?)
     * deal with here or use algebra?
     */
    
    //assume one value per scalar per record
    val vars = getOrigScalars
    val values = extractValues(record)
    
    //If we don't parse as many values as expected, assume we have an invalid record and skip it.
    if (vars.length != values.length) {
      //Note, There are many cases where datasets have invalid records "by design" 
      //(e.g. future timestamps without data values) so we don't want to log warnings.
      logger.debug("Invalid record: " + values.length + " values found for " + vars.length + " variables")
      None
    } else {
      val vnames: Seq[String] = vars.map(_.getName)
      val datas: Seq[Data] = (values zip vars).map(p => StringUtils.parseStringValue(p._1, p._2))
      Some((vnames zip datas).toMap)
    }
  }
  
  /**
   * Extract the Variable values from the given record.
   */
  def extractValues(record: String): Seq[String] = record.trim.split(getDelimiter)

}







