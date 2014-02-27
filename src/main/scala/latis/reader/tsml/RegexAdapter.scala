package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use a regular expression to extract data values from a data record.
 */
class RegexAdapter(tsml: Tsml) extends IterativeAsciiAdapter(tsml) {
  
  /**
   * Get the required regular expression pattern from the adapter definition.
   */
  val regex = getProperty("pattern") match {
    case Some(s: String) => s.r
    case None => throw new RuntimeException("RegexAdapter requires a regular expression definition 'pattern'.")
  }
    
  /**
   * Return a List of values in the given record that match
   * this Adapter's regular expression pattern.
   * Return an empty List if the record does not match (i.e. does not contain valid data).
   */
  def getMatchingValues(record: String) = {
    regex.findFirstMatchIn(record) match {
      case Some(m) => m.subgroups
      case None => List[String]()
    }
  }

  /**
   * Parse a "record" of text into a Map of Variable name to value
   * by matching this Adapter's regular expression pattern.
   */
  override def parseRecord(record: String): Map[String, String] = {
    //create Map with variable names and values
    val vnames = origScalarNames
    val values = getMatchingValues(record)
    //If we didn't find the right number of samples, drop this record by returning an empty Map
    if (vnames.length != values.length) Map[String, String]()
    else (vnames zip values).toMap
  }
}