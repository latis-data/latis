package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use a regular expression with groups to extract data values from a data record.
 * This must be defined as a 'pattern' attribute for this adapter in the tsml.
 *
 * This adapter also accepts a 'columns' attribute to rearrange the order of matches.
 */
class RegexAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {
  
  /**
   * Get the required regular expression pattern from the adapter definition.
   */
  lazy val regex = getProperty("pattern") match {
    case Some(s: String) => s.r
    case None => throw new RuntimeException("RegexAdapter requires a regular expression definition 'pattern'.")
  }

  lazy val columnIndices: Option[Seq[Array[Int]]] =
    getProperty("columns").map { s =>
      s.split(";").map(p => p.split(",").map(_.toInt))
    }

  /**
   * Return a List of values in the given record that match
   * this Adapter's regular expression pattern.
   * Return an empty List if the record does not match (i.e. does not contain valid data).
   */
  override def extractValues(record: String): Seq[String] = {
    val groups = regex.findFirstMatchIn(record) match {
      case Some(m) => m.subgroups
      case None => List[String]()
    }

    // If the 'columns' attribute was set, use the indices to reorder
    // the matches. Otherwise, just return the matches in order.
    columnIndices.map { ci =>
      if (groups.length <= ci.flatten.max) {
        // Ignore rows with fewer columns than those requested
        List()
      } else {
        // append with " " for now since delimiter could be a regex
        ci.map(is => is.map(groups(_)).mkString(" "))
      }
    }.getOrElse(groups)
  }
}
