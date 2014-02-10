package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import scala.collection._

/**
 * Use column index properties to parse ascii tabular data.
 */
class ColumnarAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {
  //TODO: any reason this couldn't be used with iterative adapter?
  
  /*
   * TODO: 2013-10-16 with Columns trait?
   * override parseRecord, delegate to super then reorder?
   * *reorder variableNames
   * what about time vars in mult cols?
   *   time as tuple? time.year, time.month...
   *   <time> 
   *     <real name="year"/>
   *     <real name="month"/>
   *   
   * 
   * Keep in mind principle of describing source dataset
   * PIs to reorder, drop,...
   *   "projection" PI?
   * complicates reordering data if dome after?
   * not as much for granule = each scalar has all data
   * some cases (e.g. time in multiple cols) better applied as parsed
   * 
   * 
   */

  //Get column numbers
  //TODO: 0 or 1 based? java is 0 but jdbc is 1, probably 0
  //TODO: from single adapter attribute, e.g. "1,2,3;5;7;9"
  //TODO: from tsml attributes, e.g. columns="1,2,3"
  //TODO: use for Vector?
  
  val indexMap: Map[String, String] = getProperty("columns") match {
    case Some(s) => (origVariableNames zip s.split(";")).toMap
    case None => throw new RuntimeException("ColumnarAdapter requires 'columns' definition.")
  }
  
  override def parseRecord(record: Record): Map[Name, Value] = {
    val names = origVariableNames
    //assume one line per record, space delimited
    val delimiter = " "  //TODO: from attributes
    val ss = record(0).split(delimiter)  //TODO: splitRecord
    //TODO: join record lines with line_delimiter?
    
    val dmap = mutable.Map[Name, Value]()
    
    for (name <- names) {
      //get values by col index and concat with space
      val value = getIndicesForVariable(name).map(ss(_)).mkString(" ")
      dmap(name) = value
    }
    
    dmap
  }
  
  def getIndicesForVariable(vname: String): Seq[Int] = indexMap(vname).split(",").map(_.toInt)
}