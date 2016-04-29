package latis.writer

import java.io.BufferedReader
import java.io.File
import java.io.FileOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import scala.io.Source
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Text
import java.io.BufferedInputStream
import java.io.FileInputStream

/**
 * Write a zip file of the files contained in a file list dataset.
 */
class ZipWriter extends Writer {
  
  def write(dataset: Dataset) = {
    
    lazy val dir = dataset.getMetadata.get("srcDir") match {
      case Some(sd) => sd + File.separator
      case None => "" //file names to zip are relative to current directory
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

        val bis = new BufferedInputStream(new FileInputStream(dir + f))
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