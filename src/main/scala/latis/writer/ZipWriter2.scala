package latis.writer

import java.io.BufferedInputStream
import java.io.File
import java.net.URL
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Text

/**
 * Write a zip file of the URLs contained in a URL list dataset.
 * This is designed to supersede ZipWriter.
 */
class ZipWriter2 extends Writer {
  //TODO: provide option to specify prefix for zip entries
  
  def write(dataset: Dataset) = {
    
    lazy val baseUrl = dataset.getMetadata.get("baseUrl") match {
      case Some(b) => b + File.separator
      case None => "" //URLs are already complete
    }
    
    //Get the name of each file as it appears in the file list.
    //Throw an exception if a 'file' Variable cannot be found in the Dataset.
    val files = dataset.findVariableByName("file") match {
      case Some(v) => dataset match {  
        case Dataset(Function(it)) => it.map(_.findVariableByName("file") match {
          case Some(Text(t)) => t
        })
      }
      case None => throw new IllegalArgumentException("ZipWriter can only write Datasets that contain a 'file' Variable.")
    }
    
    val zip = new ZipOutputStream(getOutputStream)
    try {
      for (f <- files) {
        //add zip entry to output stream
        zip.putNextEntry(new ZipEntry(f))

        val url = new URL(baseUrl + f)
        val bis = new BufferedInputStream(url.openStream())
        try {
          Stream.continually(bis.read).takeWhile(-1 !=).foreach(zip.write(_))
        }
        finally {
          bis.close
        }

        zip.closeEntry
      }
    }
    finally {
      zip.close
    }
  }
  
  override def mimeType = "application/zip"
  
}