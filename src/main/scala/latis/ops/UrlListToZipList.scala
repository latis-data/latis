package latis.ops

import java.net.URL

import latis.dm._
import latis.metadata.Metadata

/**
 * Convert a Dataset with a "url" variable into a Dataset
 * specialized for writing to a zip file: domain -> (zipEntry, url).
 *
 * This assumes that the url is absolute.
 */
class UrlListToZipList extends Operation {
  //TODO: support baseUrl
  //TODO: option for zip entry prefix or replace at specific level

  override def applyToSample(sample: Sample): Option[Sample] = sample match {
    case Sample(domain, _) => sample.findVariableByName("url") match {
      case Some(Text(resource)) => makeNameUrlPair(resource) match {
        case (name, url) =>
          Some(Sample(domain, Tuple(List(Text(Metadata("zipEntry"), name),
            Text(Metadata("url"), url)))))
      }
      case _ => throw new UnsupportedOperationException("No 'url' variable found in sample")
    }
  }
  
  /**
   * Makes a zip entry for the given URL.
   *
   * This assumes that the "url" is already absolute. It extracts only the last
   * element of the URL path for the zip entry name (which becomes the extracted
   * file name).
   */
  def makeNameUrlPair(url: String): (String, String) = {
    // Keep nothing but the file name (no scheme, path prefix, query).
    // Note that this will effectively drop a trailing "/".
    val name = new URL(url).getPath.split("/").last //TODO: malformed error, find sooner
    (name, url)
  }

}
