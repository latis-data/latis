package latis.writer

import latis.dm._
import java.net._
import java.util.zip._
import java.io._
import com.typesafe.scalalogging.LazyLogging
import latis.ops.FileListToZipList
import latis.ops.UrlListToZipList

/**
 * Proposed replacement for ZipWriter1 and ZipWriter2 (LATIS-476).
 * Use operations to transform "file" or "url" list datasets into
 * a "zip list" dataset: zipEntry -> url which this writer writes.
 * The Operations deal with the different logic for each type.
 */
class ZipWriter3 extends Writer with LazyLogging {

  def write(dataset: Dataset): Unit = {
    // Translate to "zip list" dataset based on url vs file variable
    val zds: Dataset = if (dataset.findVariableByName("url").nonEmpty)
      new UrlListToZipList()(dataset)
    else if (dataset.findVariableByName("file").nonEmpty)
      new FileListToZipList()(dataset)
    else throw new RuntimeException("No 'url' or 'file' variable found in dataset.")

    val zip = new ZipOutputStream(getOutputStream)
    try {
      zds match {
        case DatasetSamples(it) => it foreach {
          case Sample(Text(zipEntry), Text(url)) =>
            val encodedURL = ensureURLEncoded(url)

            // Open the URL input stream
            val bis: InputStream = try {
              new BufferedInputStream(new URL(encodedURL).openStream())
            } catch {
              case e: Exception =>
                val msg = s"Failed to open the URL: $url"
                throw new RuntimeException(msg, e)
            }

            // Write the zip entry
            try {
              zip.putNextEntry(new ZipEntry(zipEntry))
              Stream.continually(bis.read).takeWhile(_ != -1).foreach(zip.write(_))
            } catch {
              case e: IOException =>
                // Log a warning, leave empty entry //TODO: add error message to entry?
                val msg = s"Failed to read the URL: $url"
                logger.warn(msg, e)
            } finally {
              bis.close //make sure we release the file resource
            }
            zip.closeEntry //finalize the zip entry
          case _ => throw new RuntimeException("Not a valid zip dataset")
        }
        case _ => throw new RuntimeException("Empty dataset")
      }
      //TODO: add manifest file
    } finally {
      zip.close
    }
  }

  //TODO: net util
  /**
   * Return a URL based on the given URL that has the query encoded.
   * If the URL contains a "%" this assumes that it is already encoded.
   */
  def ensureURLEncoded(url: String): String = {
    if (!url.contains('%')) {
      url.indexOf("?") match {
        case -1    => url  //no query
        case index =>
          val qs = url.substring(index+1).split("&")
          url.substring(0, index+1) + qs.map(URLEncoder.encode(_, "UTF-8")).mkString("&")
      }
    } else url
  }

  override def mimeType: String = "application/zip"
}

object ZipWriter3 {

  def apply(out: OutputStream) = {
    val w = new ZipWriter3()
    w.setOutputStream(out)
    w
  }
}
