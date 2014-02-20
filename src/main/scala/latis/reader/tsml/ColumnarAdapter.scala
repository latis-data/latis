package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use column index properties to parse ascii tabular data.
 */
class ColumnarAdapter(tsml: Tsml) extends IterativeAsciiAdapter(tsml) {
  
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
   * Keep in mind principle of describing source dataset
   * PIs to reorder, drop,...
   * complicates reordering data if done after?
   *   not as much for granule = each scalar has all data
   * some cases (e.g. time in multiple cols) better applied as parsed
   */
  
  /**
   * Map Variable names to zero-based column index(s).
   * Each variable spec will be separated by ";" and
   * column indices for multiple column variables will
   * be separated by ",". e.g. columns="0,1,2;5;3;4"
   */
  val indexMap: Map[String, Array[Int]] = getProperty("columns") match {
    case Some(s) => {
      val names = origScalarNames
      val specs = s.split(";")
      if (names.length != specs.length) throw new Error("Must have a column specification for each variable.")
      (names zip specs.map(_.split(",").map(_.toInt))).toMap
    }
    case None => throw new RuntimeException("ColumnarAdapter requires 'columns' definition.")
  }
  
  override def parseRecord(record: String): Map[String, String] = {
    val delim = getDelimiter
    val ss = record.split(delim)

    //Helper function to build the variable value from the appropriate columns.
    //Concatenate multiple column values with the delimiter.
    def makeValue(name: String) = indexMap(name).map(ss(_)).mkString(delim)
    
    //Map variable names to their string values
    origScalarNames.map(name => (name, makeValue(name))).toMap
  }
  
}