package latis.writer

import scala.language.postfixOps

import java.io.BufferedInputStream
import java.io.File
import java.io.InputStream
import java.io.IOException
import java.net.URL
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import com.typesafe.scalalogging.LazyLogging

import latis.dm.Dataset

/**
 * Write a zip file of the files contained in a granule list dataset.
 * This depends on some naming conventions. There must be a variable
 * named "file" in the range of the Dataset's Function. Metadata may
 * include:
 *   baseUrl: Base URL to be prepended to file names.
 *   srcDir: Base directory to be prepended to file names.
 * If neither is present then it is assumed that the "file" names are
 * complete URLs.
 */
class ZipWriter2 extends Writer with LazyLogging {
  //TODO: provide option to specify root directory for zip entries
  
  def write(dataset: Dataset): Unit = {
    
    lazy val baseUrl = dataset.getMetadata.get("baseUrl") match {
      case Some(b) => b + File.separator
      case None => dataset.getMetadata.get("srcDir") match {
        case Some(dir) => "file://" + dir + File.separator
        case None => "" //URLs are already complete
      }
    }
    
    //Hack to keep full URL out of zip file. Assumes no baseUrl.
    val removePrefix = dataset.getMetadata.getOrElse("removePrefix", "")
    
    //Get the name of each file as it appears in the file list.
    //Throw an exception if a 'file' Variable cannot be found in the Dataset.
    val data = dataset.project("file").toStringMap
    val files = if (data.contains("file")) data("file")
      else throw new IllegalArgumentException("ZipWriter can only write Datasets that contain a 'file' Variable.")

    val zip = new ZipOutputStream(getOutputStream)
    try {
      for (f <- files) {
        val zipEntry = dataset.getName + File.separator + f.replaceFirst(removePrefix, "")
        val url = new URL(baseUrl + f)
        var bis: InputStream = null
        try {
          bis = new BufferedInputStream(url.openStream())
          zip.putNextEntry(new ZipEntry(zipEntry))
          Stream.continually(bis.read).takeWhile(-1 !=).foreach(zip.write(_))
        } catch {
          case e: IOException => logger.warn("No data found at: " + url, e)
        }
        finally {
          if (bis != null) bis.close
        }
        zip.closeEntry
      }
    }
    finally {
      zip.close
    }
  }
  
  override def mimeType: String = "application/zip"
  
}
