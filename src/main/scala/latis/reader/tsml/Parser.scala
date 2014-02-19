package latis.reader.tsml

//TODO: move to reader package?

import scala.io.Source
import latis.util.RegEx

trait Parser[T] {

  //type Name = String
  
  /**
   * Return an Iterator of data records.
   */
  def getRecordIterator: Iterator[T]
    
  /**
   * Return Map with Variable name to value.
   */
  def parseRecord(record: T): Map[String,T]
  
}