package latis.ops

import java.net.URL

import latis.dm._
import latis.metadata.Metadata

/**
 * Convert a Dataset with a "url" variable into a Dataset
 * specialized for writing to a zip file: zipEntry -> url.
 *
 * This assumes that the url is absolute.
 */
class UrlListToZipList extends Operation {
  //TODO: support baseUrl
  //TODO: option for zip entry prefix or replace at specific level
  
  override def apply(dataset: Dataset): Dataset = {
    dataset.project("url") match { //TODO: super GranuleList?
      case ds @ DatasetSamples(it) =>
        val samples = it.toList.flatMap(applyToSample(_))
        Dataset(Function(samples), ds.getMetadata)
      case _ => ??? //TODO: empty, possibly due to lack of "file" variable
    }
  }

  override def applyToSample(sample: Sample): Option[Sample] = sample match {
    case Sample(_, Text(resource)) => makeNameUrlPair(resource) match {
      case (name, url) => 
        Some(Sample(Text(Metadata("zipEntry"), name),
                    Text(Metadata("url"), url)))
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
