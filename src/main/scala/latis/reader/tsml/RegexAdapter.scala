package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use a regular expression to extract data values from a data record.
 */
class RegexAdapter(tsml: Tsml) extends IterativeAsciiAdapter(tsml) {
  //TODO: mixin Iterative or Granule
  
  //TODO: change att to 'pattern'?
  val regex = getProperty("regex") match {
    case Some(s: String) => s.r
    case None => throw new RuntimeException("RegexAdapter requires a regular expression definition 'regex'.")
  }
  
  /**
   * Parse a "record" of text into a Map of Variable name to value
   * by matching the given regular expression.
   */
  override def parseRecord(record: String): Map[String, String] = {
    val s = record.mkString("\n") //stitch multi-lined record back into a single string
    val values = regex.findFirstMatchIn(s) match {
      case Some(m) => m.subgroups
      case None => List[String]()
    }
    
    //create Map with variable names and values
    val vnames = origScalarNames
    //If we didn't find the right number of samples, drop this record
    if (vnames.length != values.length) Map[String, String]()
    else (vnames zip values).toMap
  }
}