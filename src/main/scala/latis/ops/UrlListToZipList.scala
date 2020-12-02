package latis.ops

import java.net.URL

import latis.dm._
import latis.metadata.Metadata

/**
 * Convert a Dataset with a "url" variable into a Dataset
 * specialized for writing to a zip file: zipEntry -> url.
 */
class UrlListToZipList extends Operation {
  //TODO: support other file separators in paths
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
   * Assume that the "url" is already absolute.
   * Extract only the file name portion of the URL
   * for the zip entry name.
   */
  def makeNameUrlPair(url: String): (String, String) = {
    val name = {
      // Keep nothing but the file name (no scheme, path prefix, query)
      val path = new URL(url).getPath //TODO: malformed error
      path.substring(path.lastIndexOf("/") + 1) //TODO: error if path ends with "/"
    }
    (name, url)
  }

}
