package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use a regular expression with groups to extract data values from a data record.
 * This must be defined as a 'pattern' attribute for this adapter in the tsml.
 */
class RegexAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {
  
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
  override def extractValues(record: String) = {
    regex.findFirstMatchIn(record) match {
      case Some(m) => m.subgroups
      case None => List[String]()
    }
  }

}