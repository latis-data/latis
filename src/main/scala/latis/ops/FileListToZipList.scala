package latis.ops

import java.nio.file.Paths

import latis.dm._
import latis.metadata.Metadata

/**
 * Convert a Dataset with a "file" variable into a Dataset
 * specialized for writing to a zip file: zipEntry -> url.
 */
class FileListToZipList extends Operation {
  //TODO: support other file separators in paths
  //TODO: option for zip entry prefix or replace at diff level than crawl
  //TODO: pass srcDir as arg to keep things pure?

  // Keep a copy so other methods can get at the Dataset.
  private var _dataset : Dataset = null

  override def apply(dataset: Dataset): Dataset = {
    _dataset = dataset
    dataset.project("file") match { //TODO: super GranuleList?
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
   * Optional source directory from the dataset metadata.
   */
  lazy val srcDir: Option[String] =
    _dataset.getMetadata.get("srcDir")

  /**
   * Use the "file" value as the zip entry.
   * If "srcDir" is defined, assume that the file paths are
   * relative to that and combine them to get the fully
   * resolved path. 
   * If "srcDir" is not defined, assume that the file path
   * is fully resolved.
   * Prepend the "file" scheme to make a URL.
   */
  def makeNameUrlPair(file: String): (String, String) = {
    val url = srcDir match {
      //join with separator without duplication
      case Some(base) => Paths.get(base, file).toString
      case None       => file
    }
    (file, "file://" + url)
  }

}
