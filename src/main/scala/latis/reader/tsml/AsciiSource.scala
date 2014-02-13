package latis.reader.tsml

import scala.io.Source
import scala.collection._
import latis.reader.tsml.ml.Tsml

class AsciiSource(tsml: Tsml) extends GranuleAdapter(tsml) {
  
  /*
   * TODO: combine granule vs iterative and ascii vs binary
   * all could be based on: recordIterator, parseRecord
   * consider cache ~ granule
   *   storable with ehcache?
   *   consistent with DataMap util
   * consider data values in tsml
   * 
   * traits: ascii/bin or granule/iterative?
   * can't have state
   * should every adapter have a 'cache'?
   *   granlule and tsml values could use it
   *  
   * 
   */
  
  //TODO: could we use the same approach for binary data? define these as ByteBuffer instead of String?
  
  /*
   * TODO: 2013-10-07
   * Could this be designed so the granule adapter simply iterates over IterativeAdapter?
   * is there ever a need for granule? just a matter of caching?
   * granule adapter uses column oriented store, sep seq for each var
   * iterative adapter stitches them together in samples
   * 
   * How far can we go with parseRecord abstraction?
   * binary data "records", ByteBuffer instead of String?
   * use traits for ascii vs bin or granlue vs interative?
   * http://mods-jira.lasp.colorado.edu:8080/browse/LATIS-29
   */
  
  //---- Handy type aliases for a String when we are using it in the context of variable names and values.

  /**
   * Used for the name of a Variable as defined by the "name" attribute in the TSML, presumably a Scalar.
   */
  type Name = String
  
  /**
   * Used for the data value representation of a Variable, presumably a Scalar.
   */
  type Value = String
  
  /**
   * Used for a Line of ASCII text from the data source.
   */
  type Line = String
  
  /**
   * A record may be one or more lines.
   */
  type Record = Seq[Line]
  /*
   * TODO: do we need this? just use string
   * linesPerRecord
   * just a matter of appending/splitting on newline (\n)?
   * we even assume one line per record below
   * IterativeAscii recordIterator uses it
   * just mkString("\n") as needed?
   *   use delimiter? so split works?
   *   that assumes a value doesn't cross line boundaries
   *   can't be used for iterative, realizes all samples
   *   "take(linesPerRecord)"?
   */
  
  /**
   * Keep track of the data Source so we can close it.
   */
  lazy private val source: Source = Source.fromURL(getUrl())
  
  /**
   * Get the character that is used at the start of a line to indicate that
   * it should not be read as data. Defaults to null, meaning that no line
   * should be ignored (except empty lines).
   */
  lazy private val commentCharacter: String = getProperty("commentCharacter") match {
    case Some(s) => s
    case None => null
  }
  
  /**
   * Return Iterator of lines, filter out lines deemed unworthy by "shouldSkipLine",
   * including empty lines (addresses problem at end of file) and lines starting 
   * with the "commentCharacter".
   * This is "lazy" so we won't access the data source until called upon.
   */
  lazy val lineIterator: Iterator[Line] = source.getLines().filterNot(shouldSkipLine(_))
  
  /**
   * This method will be used by the lineIterator to skip lines from the data source
   * that we don't want in the data. Adapters could override this to add special rules
   * but shouldn't bypass the existing ones. 
   * Note that the "isEmpty" test bypasses an end of file problem iterating over the 
   * iterator from Source.getLines.
   */
  def shouldSkipLine(line: Line): Boolean = {
    line.isEmpty() || (commentCharacter != null && line.startsWith(commentCharacter))
  }
  
  /**
   * This Iterator will return multiple lines of text for each record
   * as defined by the "linesPerRecord" attribute in the TSML.
   * Instead of merging all the lines into a single String, each sample
   * will be Seq of lines so the subclass can interpret it as it will.
   */
  def recordIterator: Iterator[Record] = lineIterator.grouped(linesPerRecord)
  
  /**
   * The "linesPerRecord" attribute from this Adapter's definition in the TSML.
   * This value represents how many lines of text in the ASCII source are needed
   * for one record/sample of the outer Function.
   */
  val linesPerRecord: Int = getProperty("linesPerRecord") match {
    case Some(s) => s.toInt
    case None => 1
  }
  
  
  /**
   * Subclasses should implement this method to parse a "record" of text
   * into a Map of Variable name to value. This may be one or more lines
   * as defined by the "linesPerRecord" attribute of this adapter definition 
   * in the TSML.
   * Note, LinkedHashMap will maintain order.
   * Return empty Map if there was a problem with this record.
   * The metadata is for the Function Variable that we are iterating over.
   */
  //def parseRecord(metadata: FunctionMd, record: Record): LinkedHashMap[Name,ArrayBuffer[Value]]
  def parseRecord(record: Record): Map[Name, Value] = {
    //assume one line per record, space delimited
    (origVariableNames zip record.head.split(" ")).toMap
  }
  
//  //suck in entire granule, for now
//  def readData: immutable.Map[Name, immutable.Seq[Value]] = {
//    val map = initDataMap
//        
//    val it = recordIterator
//    while (it.hasNext) {
//      val record = it.next
//      val vs = parseRecord(record)
//      //skip bad records (empty Map)
//      if (vs.nonEmpty) for (vname <- origVariableNames) map(vname) append vs(vname)
//    }
//    
//    immutableDataMap(map)
//  }
  
  
  def close = source.close
}