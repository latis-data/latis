package latis.writer

import java.io._
import java.net._
import java.util.zip._

import com.typesafe.scalalogging.LazyLogging
import latis.dm._
import latis.ops.FileListToZipList
import latis.ops.UrlListToZipList

/**
 * Proposed replacement for ZipWriter1 and ZipWriter2 (LATIS-476).
 * Use operations to transform "file" or "url" list datasets into
 * a "zip list" dataset: zipEntry -> url which this writer writes.
 * The Operations deal with the different logic for each type.
 *
 * Duplicate zip entries will be disambiguated by appending
 * a counter with an underscore (e.g. "_1").
 */
class ZipWriter3 extends Writer with LazyLogging {
  //TODO: add manifest
  //TODO: add disambiguating "_1" before file extension

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
            // Open the URL input stream
            val bis: InputStream = try {
              new BufferedInputStream(new URL(url).openStream())
            } catch {
              case e: Exception =>
                val msg = s"Failed to open the URL: $url"
                throw new RuntimeException(msg, e)
            }

            // Write the zip entry
            try {
              zip.putNextEntry(new ZipEntry(disambiguate(zipEntry)))
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

  /**
   * Keeps a counter so we can disambiguate duplicate zip entry names.
   * We do it this way so we can stream samples.
   */
  private val counter = scala.collection.mutable.Map[String, Int]()

  /**
   * Makes sure that we don't duplicate zip entry names.
   * This will append "_n" for names that have already been used
   * where "n" is a counter starting at 1. A novel name will be unchanged.
   */
  private def disambiguate(name: String): String = counter.get(name) match {
    case Some(n) =>
      counter += ((name, n + 1))
      name + "_" + n
    case None =>
      counter += ((name, 1))
      name
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
