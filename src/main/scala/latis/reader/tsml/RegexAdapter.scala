package latis.reader.tsml

/**
 * Use a regular expression to extract data values from a data record.
 */
class RegexAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {
  
  val regex = properties.get("regex") match {
    case Some(s: String) => s.r
    case None => throw new RuntimeException("RegexAdapter requires a regular expression definition 'regex'.")
  }
  
  /**
   * Parse a "record" of text into a Map of Variable name to value. 
   * This may be one or more lines as defined by the "linesPerRecord" 
   * attribute of this adapter definition in the TSML.
   * Note, LinkedHashMap will maintain order.
   * Return empty Map if there was a problem with this record.
   */
  override def parseRecord(record: Record): Map[Name, Value] = {
    val s = record.mkString("\n") //stitch multi-lined record back into a single string
    val values = regex.findFirstMatchIn(s) match {
      case Some(m) => m.subgroups
      case None => List[String]()
    }
    
    //create Map with variable names and values
    val vnames = variableNames
    //If we didn't find the right number of samples, drop this record
    if (vnames.length != values.length) Map[Name, Value]()
    else (vnames zip values).toMap
  }
}