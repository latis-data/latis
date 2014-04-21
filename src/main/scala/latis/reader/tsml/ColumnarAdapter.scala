package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use column index properties to parse ascii tabular data.
 */
class ColumnarAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {  
  
  lazy val columnIndices: Seq[Array[Int]] = getProperty("columns") match {
    case Some(s: String) => s.split(";").map(p => p.split(",").map(_.toInt))
    case None => throw new RuntimeException("ColumnarAdapter requires 'columns' definition.")
  }
  
  override def extractValues(record: String): Seq[String] = {
    val ss = record.split(getDelimiter)
    columnIndices.map(is => is.map(ss(_)).mkString(" ")) //append with " " for now since delimiter could be a regex
  }
  
  /**
   * Map Variable names to zero-based column index(s).
   * Each variable spec will be separated by ";" and
   * column indices for multiple column variables will
   * be separated by ",". e.g. columns="0,1,2;5;3;4"
   */
//  val indexMap: Map[String, Array[Int]] = getProperty("columns") match {
//    case Some(s) => {
//      val names = getOrigScalarNames
//      val specs = s.split(";")
//      if (names.length != specs.length) throw new Error("Must have a column specification for each variable.")
//      (names zip specs.map(_.split(",").map(_.toInt))).toMap
//    }
//    case None => throw new RuntimeException("ColumnarAdapter requires 'columns' definition.")
//  }
//  
//  override def parseRecord(record: String): Map[String, String] = {
//    val delim = getDelimiter
//    val ss = record.split(delim)
//
//    //Helper function to build the variable value from the appropriate columns.
//    //Concatenate multiple column values with the delimiter.
//    //TODO: problem if delim is regex, just use " " for now.
//    //  needed for time format (units) for example: "yy MM" vs "yy  MM"
////TODO: deal with invalid record, ss.length? return empty map like regex adapter
//    def makeValue(name: String) = indexMap(name).map(ss(_)).mkString(" ")
//    
//    //Map variable names to their string values
//    getOrigScalarNames.map(name => (name, makeValue(name))).toMap
//  }
  
}