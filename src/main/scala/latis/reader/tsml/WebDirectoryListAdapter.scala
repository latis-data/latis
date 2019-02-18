package latis.reader.tsml

import java.net.URLDecoder

import latis.reader.tsml.ml.Tsml

/**
 * Scrapes a dataset of files from a web-based directory listing.
 *
 * The 'pattern' attribute in the TSML is expected to define what file
 * names in this directory listing look like and what values should be
 * extracted from the file name. The pattern can contain more than
 * just the file name if necessary. We assume that the first capture
 * group in the regular expression is the whole file name.
 *
 * The only interesting thing this class adds over [[RegexAdapter]] is
 * that we add the full URL to the file to the end of the sequence of
 * extracted values.
 */
class WebDirectoryListAdapter(tsml: Tsml) extends RegexAdapter(tsml) {

  /**
   * The directory we want to get a file list from.
   *
   * This is determined by the 'location' attribute in the TSML.
   */
  lazy val directory = URLDecoder.decode(getUrl.toString, "UTF-8")

  /**
   * Extract a sequence of values from a file name.
   *
   * The final element of the sequence of values will be the full URL
   * to the file.
   */
  override def extractValues(record: String): Seq[String] = {
    // Extract values from the file name using the regular expression.
    val groups = super.extractValues(record)

    if (groups.nonEmpty) {
      // Construct the full URL to the file. We assume that the file
      // name is the first capturing group.
      val url = s"$directory/${groups(0)}"

      // We assume the URL is defined last.
      groups.tail :+ url
    } else {
      groups
    }
  }
}
